
# -------------------- SETUP --------------------
suppressPackageStartupMessages({
  library(jsonlite)
  library(dplyr)
  library(readr)
  library(stringr)
  library(stringi)
  library(lubridate)
  library(readxl)
  library(tidyr)
  library(purrr)
})

CONFIG <- list(
  # Parâmetros de processamento
  processing = list(
    min_chars_text    = 2L,
    tz_local          = "America/Fortaleza",
    filter_empty_text = TRUE,
    deduplicate       = FALSE,
    write_parquet     = requireNamespace("arrow", quietly = TRUE),
    
    # Subtipos considerados ruído (channel_archive removido)
    noise_subtypes = c(
      "channel_join", "channel_leave", "message_deleted",
      "channel_topic", "channel_purpose", "file_share",
      "message_replied", "bot_add", "bot_remove",
      "channel_convert_to_public", "channel_name", "reminder_add"
    )
  ),
  
  # -------------------- DATAS DE ARQUIVAMENTO DE CANAIS --------------------
  # INSTRUÇÕES: Coloque aqui a data de arquivamento de cada canal
  # Formato: "YYYY-MM-01" (primeiro dia do mês)
  # Canais não arquivados: use NA
  
  channel_archive_dates = list(
    alpha = list(
      "backend-arquitetura" = "2023-09-01",
      "helpdesk-thedayafter" = "2023-12-01",
      "segurança-produto" = "2023-10-01"
    ),
    beta = list(
    )
  ),
  
  # Paths dos projetos
  projects = list(
    alpha = list(
      root = "/Users/user/Slack_Analysis/Slack_Data/alpha/alpha_chat_logs",
      results_dir = "/Users/user/Slack_Analysis/Slack_Data/alpha/alpha_results",
      channels_xlsx = "/Users/user/Slack_Analysis/Slack_Data/alpha/alpha_channel_metrics/alpha_channels.xlsx",
      users_xlsx = "/Users/user/Slack_Analysis/Slack_Data/alpha/alpha_user_metrics/alpha_users.xlsx"
    ),
    beta = list(
      root = "/Users/user/Slack_Analysis/Slack_Data/beta/beta_chat_logs",
      results_dir = "/Users/user/Slack_Analysis/Slack_Data/beta/beta_results",
      channels_xlsx = "/Users/user/Slack_Analysis/Slack_Data/beta/beta_channel_metrics/beta_channels.xlsx",
      users_xlsx = "/Users/user/Slack_Analysis/Slack_Data/beta/beta_user_metrics/beta_users.xlsx"
    )
  )
)

# -------------------- UTILS --------------------
`%||%` <- function(a, b) if (is.null(a) || length(a) == 0) b else a

safe_fromJSON <- function(path) {
  tryCatch(fromJSON(path, simplifyVector = FALSE), error = function(e) NULL)
}

to_posix_from_ts <- function(ts_chr, tz = "UTC") {
  x <- suppressWarnings(as.numeric(ts_chr))
  x[!is.finite(x)] <- NA_real_
  as.POSIXct(floor(x), origin = "1970-01-01", tz = tz)
}

sanitize_fname <- function(x) {
  str_replace_all(x, "[^A-Za-z0-9-_]+", "_") %>% str_squish()
}

normalize_text <- function(x) {
  if (is.null(x) || length(x) == 0) return(character(0))
  x %>%
    stri_trans_nfc() %>%
    stri_replace_all_regex("\\p{Cf}+", "") %>%
    str_replace_all("[ \t\r\n]+", " ") %>%
    trimws()
}

norm_str <- function(x) {
  x %>%
    as.character() %>%
    stri_replace_all_regex("\\p{Cf}+", "") %>%
    {gsub("\\u00A0", " ", ., useBytes = TRUE)} %>%
    stri_trans_general("Latin-ASCII") %>%
    stri_replace_all_regex("\\p{Pd}+", "-") %>%
    stri_replace_all_regex("\\s+", " ") %>%
    trimws() %>%
    tolower()
}

ensure_month_key <- function(df, col = "month", format_type = c("iso", "excel")) {
  format_type <- match.arg(format_type)
  x <- df[[col]]
  
  mk <- if (format_type == "iso") {
    posix <- suppressWarnings(ymd_hms(x, tz = "UTC", quiet = TRUE))
    posix[is.na(posix)] <- suppressWarnings(ymd(x[is.na(posix)], tz = "UTC", quiet = TRUE))
    posix
  } else {
    if (inherits(x, c("Date", "POSIXt"))) {
      as.POSIXct(x)
    } else {
      coalesce(
        suppressWarnings(dmy(paste0("01/", x), quiet = TRUE)),
        suppressWarnings(my(x, quiet = TRUE)),
        suppressWarnings(ymd(paste0(x, "-01"), quiet = TRUE))
      )
    }
  }
  
  df$month_key <- as.Date(floor_date(mk, "month"))
  df
}

month_key_to_isoz <- function(d) {
  paste0(format(as.Date(d), "%Y-%m-01"), "T00:00:00Z")
}

first_non_empty <- function(x) {
  x <- as.character(x)
  ix <- which(!is.na(x) & nzchar(trimws(x)))
  if (length(ix)) x[ix[1]] else NA_character_
}

# -------------------- EXTRAÇÃO SLACK --------------------
extract_blocks_text <- function(msg) {
  if (is.null(msg$blocks)) return(NA_character_)
  
  texts <- character()
  for (b in msg$blocks) {
    if (is.null(b$elements)) next
    for (e in b$elements) {
      if (!is.null(e$text)) texts <- c(texts, e$text)
      if (!is.null(e$elements)) {
        for (ee in e$elements) {
          if (!is.null(ee$text)) texts <- c(texts, ee$text)
        }
      }
    }
  }
  if (length(texts)) paste(texts, collapse = " ") else NA_character_
}

slack_json_to_dataframe <- function(slack_json) {
  if (is.null(slack_json) || !length(slack_json)) {
    return(tibble(
      message_id = character(), timestamp = character(), 
      user_id = character(), message_type = character(), 
      text = character(), reply_count = integer(),
      reply_users_count = integer(), latest_reply_ts = character(),
      thread_ts = character(), parent_user_id = character()
    ))
  }
  
  map_dfr(slack_json, function(msg) {
    text_raw <- msg$text %||% NA_character_
    if (is.na(text_raw) || text_raw == "") {
      text_raw <- extract_blocks_text(msg)
    }
    
    subtype <- msg$subtype
    type_final <- if (!is.null(subtype)) subtype else (msg$type %||% NA_character_)
    
    user <- coalesce(
      msg$user, msg$bot_id, 
      if (!is.null(msg$message)) msg$message$user else NULL,
      msg$username
    ) %||% NA_character_
    
    ts <- msg$ts %||% NA_character_
    
    tibble(
      message_id = if (!is.na(ts)) {
        paste0(ts, "_", ifelse(is.na(user), "nouser", user))
      } else {
        paste0("nokey_", ifelse(is.na(user), "nouser", user))
      },
      timestamp = ts,
      user_id = user,
      message_type = type_final,
      text = text_raw,
      reply_count = as.integer(msg$reply_count %||% NA),
      reply_users_count = as.integer(msg$reply_users_count %||% NA),
      latest_reply_ts = msg$latest_reply %||% NA_character_,
      thread_ts = msg$thread_ts %||% NA_character_,
      parent_user_id = msg$parent_user_id %||% NA_character_
    )
  })
}

# -------------------- DESCOBERTA DE CANAIS --------------------
discover_channel_dirs <- function(root_dir) {
  dirs <- list.dirs(root_dir, full.names = FALSE, recursive = FALSE)
  keep <- vapply(dirs, function(d) {
    full <- file.path(root_dir, d)
    if (!dir.exists(full)) return(FALSE)
    any(grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}\\.json$", 
              list.files(full, full.names = FALSE)))
  }, logical(1))
  dirs[keep]
}

list_dayfiles <- function(root_dir, channel_name) {
  p <- file.path(root_dir, channel_name)
  if (!dir.exists(p)) return(character(0))
  files <- list.files(p, pattern = "^[0-9]{4}-[0-9]{2}-[0-9]{2}\\.json$", 
                      full.names = FALSE)
  sort(files, method = "radix")
}

augment_channels <- function(root_dir) {
  channels_path <- file.path(root_dir, "channels.json")
  channels_json <- safe_fromJSON(channels_path) %||% list()
  
  channels_json <- map(channels_json, function(ch) {
    ch$dayslist <- list_dayfiles(root_dir, ch$name %||% NA_character_)
    ch
  })
  
  folders_with_logs <- discover_channel_dirs(root_dir)
  existing_names <- map_chr(channels_json, ~ .x$name %||% NA_character_)
  existing_names <- existing_names[!is.na(existing_names)]
  missing_channels <- setdiff(folders_with_logs, existing_names)
  
  synthetic <- map(missing_channels, function(nm) {
    list(
      id = NA, name = nm, created = NA, creator = NA,
      is_archived = NA, is_general = NA, members = NULL,
      topic = list(value = NULL), purpose = list(value = NULL),
      dayslist = list_dayfiles(root_dir, nm)
    )
  })
  
  c(channels_json, synthetic)
}

# -------------------- PROCESSAMENTO PRINCIPAL --------------------
process_slack_project <- function(project_config, key) {
  cat("\n", strrep("=", 60), "\n")
  cat("📊 Processando projeto:", toupper(key), "\n")
  cat(strrep("=", 60), "\n")
  
  root_dir <- project_config$root
  results_dir <- project_config$results_dir
  
  if (!dir.exists(root_dir)) stop("Diretório raiz não existe: ", root_dir)
  dir.create(results_dir, showWarnings = FALSE, recursive = TRUE)
  
  # 1. CHANNELS
  cat("🔍 Augmentando channels.json...\n")
  channels_json <- augment_channels(root_dir)
  
  write_json(channels_json, 
             file.path(results_dir, paste0(key, "_channels_augmented.json")),
             auto_unbox = TRUE, pretty = TRUE)
  
  channels_df <- map_dfr(channels_json, function(ch) {
    tibble(
      channel_id = ch$id %||% NA_character_,
      channel_name = ch$name %||% NA_character_,
      channel_created_ts = ch$created %||% NA,
      channel_creator_id = ch$creator %||% NA_character_,
      is_archived = ch$is_archived %||% NA,
      is_general = ch$is_general %||% NA,
      member_ids = if (!is.null(ch$members) && length(ch$members)) {
        paste(ch$members, collapse = ", ")
      } else NA_character_,
      channel_topic = if (!is.null(ch$topic) && length(ch$topic$value)) {
        ch$topic$value
      } else NA_character_,
      channel_purpose = if (!is.null(ch$purpose) && length(ch$purpose$value)) {
        ch$purpose$value
      } else NA_character_
    )
  })
  
  # 2. USERS
  cat("👥 Carregando users.json...\n")
  users_path <- file.path(root_dir, "users.json")
  users_json <- safe_fromJSON(users_path) %||% list()
  
  users_df <- map_dfr(users_json, function(u) {
    tibble(
      user_id = u$id %||% NA_character_,
      team_id = u$team_id %||% NA_character_,
      user_name = u$name %||% NA_character_,
      is_deleted = u$deleted %||% NA,
      real_name = coalesce(
        u$real_name,
        if (!is.null(u$profile)) u$profile$real_name else NULL
      ) %||% NA_character_,
      timezone = u$tz %||% NA_character_,
      timezone_label = u$tz_label %||% NA_character_,
      timezone_offset = u$tz_offset %||% NA,
      user_title = if (!is.null(u$profile)) {
        u$profile$title %||% NA_character_
      } else NA_character_,
      display_name = if (!is.null(u$profile)) {
        u$profile$display_name %||% NA_character_
      } else NA_character_,
      is_bot = u$is_bot %||% NA
    )
  })
  
  # 3. MENSAGENS
  cat("💬 Processando mensagens (", length(channels_json), " canais)...\n")
  pb <- txtProgressBar(min = 0, max = length(channels_json), style = 3)
  
  messages_df <- map_dfr(seq_along(channels_json), function(i) {
    ch <- channels_json[[i]]
    setTxtProgressBar(pb, i)
    
    if (!length(ch$dayslist)) return(NULL)
    
    ch_messages <- map_dfr(ch$dayslist, function(dayfile) {
      path <- file.path(root_dir, ch$name, dayfile)
      json <- safe_fromJSON(path)
      if (is.null(json)) return(NULL)
      slack_json_to_dataframe(json)
    })
    
    if (nrow(ch_messages)) {
      ch_messages$channel_name <- ch$name
      ch_messages
    } else {
      NULL
    }
  })
  
  close(pb)
  
  before_filters_n <- nrow(messages_df)
  
  # 4. TRANSFORMAÇÕES
  if (nrow(messages_df)) {
    cat("\n🧹 Aplicando filtros e transformações...\n")
    
    messages_df$text <- normalize_text(messages_df$text)
    
    if (CONFIG$processing$filter_empty_text) {
      messages_df <- messages_df %>%
        filter(!is.na(text), nzchar(text), nchar(text) >= CONFIG$processing$min_chars_text)
    }
    
    messages_df <- messages_df %>%
      filter(!(message_type %in% CONFIG$processing$noise_subtypes))
    
    messages_df <- messages_df %>%
      mutate(
        timestamp_utc = to_posix_from_ts(timestamp, tz = "UTC"),
        timestamp_local = with_tz(timestamp_utc, tzone = CONFIG$processing$tz_local),
        date_local = as.Date(timestamp_local),
        year = year(timestamp_local),
        month = month(timestamp_local),
        day = day(timestamp_local),
        weekday = wday(timestamp_local, label = TRUE),
        hour = hour(timestamp_local)
      )
    
    if (nrow(users_df)) {
      messages_df <- messages_df %>%
        left_join(
          users_df %>% select(user_id, display_name, real_name, user_name, user_title, is_bot),
          by = "user_id"
        )
    } else {
      messages_df <- messages_df %>%
        mutate(user_title = NA_character_, is_bot = NA)
    }
    
    messages_df <- messages_df %>%
      mutate(
        is_bot = coalesce(is_bot, FALSE) |
          (tolower(message_type) == "bot_message") |
          (user_id == "USLACKBOT"),
        is_human = !is_bot
      )
    
    messages_df <- messages_df %>%
      mutate(
        is_thread_root = !is.na(thread_ts) & thread_ts == timestamp,
        is_thread_reply = !is.na(thread_ts) & nzchar(thread_ts) & thread_ts != timestamp
      )
    
    reply_counts <- messages_df %>%
      filter(is_thread_reply) %>%
      count(channel_name, thread_ts, name = "reply_count_calculated")
    
    messages_df <- messages_df %>%
      left_join(reply_counts, by = c("channel_name", "timestamp" = "thread_ts")) %>%
      mutate(
        reply_count_final = coalesce(reply_count_calculated, reply_count),
        text_length = nchar(text)
      ) %>%
      select(-reply_count_calculated)
    
    messages_df <- messages_df %>% arrange(channel_name, timestamp_utc)
  }
  
  after_filters_n <- nrow(messages_df)
  
  # 5. EXPORTS PRINCIPAIS
  cat("💾 Exportando arquivos principais...\n")
  
  if (nrow(messages_df)) {
    write_csv(messages_df, 
              file.path(results_dir, paste0(key, "_slack_messages.csv")), 
              na = "")
    
    if (CONFIG$processing$write_parquet) {
      arrow::write_parquet(messages_df, 
                           file.path(results_dir, paste0(key, "_slack_messages.parquet")))
    }
  }
  
  write_csv(users_df, file.path(results_dir, paste0(key, "_users.csv")), na = "")
  write_csv(channels_df, file.path(results_dir, paste0(key, "_channels.csv")), na = "")
  
  # 6. MÉTRICAS AUXILIARES
  if (nrow(messages_df)) {
    cat("📊 Gerando métricas auxiliares...\n")
    
    msgs_by_user <- messages_df %>%
      filter(is_human) %>%
      count(channel_name, user_id, display_name, real_name, 
            user_name, user_title, name = "message_count") %>%
      arrange(desc(message_count))
    
    write_csv(msgs_by_user, 
              file.path(results_dir, paste0(key, "_messages_by_user.csv")))
  }
  
  # 7. MÉTRICAS MENSAIS
  channel_metrics <- NULL
  user_metrics <- NULL
  title_metrics_by_channel <- NULL
  
  if (nrow(messages_df)) {
    cat("📈 Calculando métricas mensais...\n")
    
    messages_monthly <- messages_df %>%
      mutate(month_start = floor_date(timestamp_local, "month"))
    
    get_archive_status <- function(channel_name, month_date, project_key) {
      archive_dates <- CONFIG$channel_archive_dates[[project_key]]
      if (is.null(archive_dates) || !channel_name %in% names(archive_dates)) {
        return(FALSE)
      }
      
      archive_date <- archive_dates[[channel_name]]
      if (is.na(archive_date)) {
        return(FALSE)
      }
      
      as.Date(month_date) >= as.Date(archive_date)
    }
    
    channel_metrics <- messages_monthly %>%
      group_by(month_start, channel_name) %>%
      summarise(
        message_count = n(),
        message_count_bots = sum(is_bot, na.rm = TRUE),
        message_count_humans = message_count - message_count_bots,
        unique_users = n_distinct(user_id[is_human]),
        thread_count = sum(is_thread_root, na.rm = TRUE),
        reply_count = sum(is_thread_reply, na.rm = TRUE),
        avg_text_length = mean(text_length, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      left_join(
        channels_df %>% select(channel_id, channel_name, channel_created_ts),
        by = "channel_name"
      ) %>%
      mutate(
        month = month_key_to_isoz(month_start),
        is_archived = mapply(get_archive_status, channel_name, month_start, 
                             MoreArgs = list(project_key = key))
      ) %>%
      select(month, channel_id, channel_name, channel_created_ts, 
             is_archived, message_count, message_count_humans, 
             message_count_bots, unique_users, thread_count, 
             reply_count, avg_text_length) %>%
      arrange(month, channel_name)
    
    user_metrics <- messages_monthly %>%
      filter(!is_bot) %>%
      group_by(month_start, user_id, display_name, real_name, 
               user_name, user_title) %>%
      summarise(
        public_message_count = n(),
        unique_channels = n_distinct(channel_name),
        thread_starts = sum(is_thread_root, na.rm = TRUE),
        thread_replies = sum(is_thread_reply, na.rm = TRUE),
        avg_text_length = mean(text_length, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(month = month_key_to_isoz(month_start)) %>%
      select(month, user_id, display_name, real_name, user_name, 
             user_title, public_message_count, unique_channels, 
             thread_starts, thread_replies, avg_text_length) %>%
      arrange(month, desc(public_message_count))
    
    title_metrics_by_channel <- messages_monthly %>%
      filter(!is_bot, !is.na(user_title), nzchar(user_title)) %>%
      left_join(
        channels_df %>% select(channel_id, channel_name),
        by = "channel_name"
      ) %>%
      group_by(month_start, channel_id, channel_name, user_title) %>%
      summarise(
        message_count = n(),
        unique_users = n_distinct(user_id),
        .groups = "drop"
      ) %>%
      mutate(month = month_key_to_isoz(month_start)) %>%
      select(month, channel_id, channel_name, user_title, 
             message_count, unique_users) %>%
      arrange(month, channel_name, desc(message_count))
    
    write_csv(channel_metrics, 
              file.path(results_dir, paste0(key, "_channel_metrics_monthly.csv")))
    write_csv(user_metrics, 
              file.path(results_dir, paste0(key, "_user_metrics_monthly.csv")))
    write_csv(title_metrics_by_channel, 
              file.path(results_dir, paste0(key, "_title_metrics_by_channel_monthly.csv")))
  }
  
  # 8. RESUMO
  cat("\n📋 Resumo:", toupper(key), "\n")
  cat(sprintf("  Mensagens antes filtros: %d\n", before_filters_n))
  cat(sprintf("  Mensagens após filtros: %d\n", after_filters_n))
  if (nrow(messages_df)) {
    cat(sprintf("  Humanos: %d | Bots: %d\n", 
                sum(messages_df$is_human), sum(messages_df$is_bot)))
  }
  
  list(
    key = key,
    messages = messages_df,
    users = users_df,
    channels = channels_df,
    channel_metrics = channel_metrics,
    user_metrics = user_metrics,
    title_metrics_by_channel = title_metrics_by_channel
  )
}

# -------------------- ENRIQUECIMENTO --------------------
enrich_channel_metrics <- function(csv_path, xlsx_path, key) {
  if (!file.exists(csv_path) || !file.exists(xlsx_path)) {
    warning("Arquivo não encontrado para enriquecimento de canais")
    return(NULL)
  }
  
  cat("🔗 Enriquecendo channel metrics...\n")
  
  base <- read_csv(csv_path, show_col_types = FALSE) %>%
    ensure_month_key(format_type = "iso") %>%
    mutate(channel_name_norm = norm_str(channel_name))
  
  external <- read_xlsx(xlsx_path) %>%
    ensure_month_key(col = "data", format_type = "excel") %>%
    transmute(
      month_key,
      channel_name = nome_canal,
      channel_name_norm = norm_str(nome_canal),
      members_who_viewed = as.integer(membros_queviram),
      reactions_count = as.integer(reacoes),
      members_who_reacted = as.integer(membros_quereagiram)
    ) %>%
    group_by(month_key, channel_name_norm) %>%
    summarise(across(where(is.numeric), ~sum(.x, na.rm = TRUE)),
              channel_name = first(channel_name),
              .groups = "drop")
  
  enriched <- full_join(base, external, by = c("month_key", "channel_name_norm")) %>%
    mutate(
      month = month_key_to_isoz(month_key),
      channel_name = coalesce(channel_name.x, channel_name.y)
    ) %>%
    select(-ends_with(".x"), -ends_with(".y"), -channel_name_norm, -month_key) %>%
    arrange(month, channel_name)
  
  write_csv(enriched, csv_path, na = "")
  enriched
}

enrich_user_metrics <- function(csv_path, xlsx_path, key) {
  if (!file.exists(csv_path) || !file.exists(xlsx_path)) {
    warning("Arquivo não encontrado para enriquecimento de usuários")
    return(NULL)
  }
  
  cat("🔗 Enriquecendo user metrics...\n")
  
  base <- read_csv(csv_path, show_col_types = FALSE) %>%
    ensure_month_key(format_type = "iso") %>%
    mutate(user_id = as.character(user_id))
  
  external <- read_xlsx(xlsx_path) %>%
    ensure_month_key(col = "data", format_type = "excel") %>%
    transmute(
      month_key,
      user_id = as.character(user_id),
      real_name = nome,
      days_active = as.integer(dias_ativo),
      messages_posted_external = as.integer(mensagens_postadas),
      messages_in_channels = as.integer(mensagens_postadas_emcanais),
      reactions_count = as.integer(reacoes)
    ) %>%
    group_by(month_key, user_id) %>%
    summarise(across(where(is.numeric), ~sum(.x, na.rm = TRUE)),
              real_name = first(real_name),
              .groups = "drop")
  
  fill_lookup <- base %>%
    group_by(user_id) %>%
    summarise(
      display_name_base = first_non_empty(display_name),
      user_name_base = first_non_empty(user_name),
      user_title_base = first_non_empty(user_title),
      .groups = "drop"
    )
  
  enriched <- full_join(base, external, by = c("month_key", "user_id")) %>%
    filter(user_id %in% external$user_id) %>%
    left_join(fill_lookup, by = "user_id") %>%
    mutate(
      month = month_key_to_isoz(month_key),
      display_name = coalesce(display_name, display_name_base),
      user_name = coalesce(user_name, user_name_base),
      user_title = coalesce(user_title, user_title_base),
      real_name = coalesce(real_name.y, real_name.x),
      private_message_count = pmax(0, messages_posted_external - messages_in_channels, na.rm = TRUE),
      total_messages = public_message_count + private_message_count
    ) %>%
    select(-ends_with("_base"), -ends_with(".x"), -ends_with(".y"), -month_key) %>%
    select(month, user_id, display_name, real_name, user_name, user_title,
           public_message_count, private_message_count, total_messages,
           unique_channels, thread_starts, thread_replies, avg_text_length,
           days_active, messages_posted_external, messages_in_channels, 
           reactions_count) %>%
    arrange(month, desc(total_messages))
  
  write_csv(enriched, csv_path, na = "")
  enriched
}

# -------------------- PIPELINE COMPLETO --------------------
execute_pipeline <- function() {
  
  results <- map(names(CONFIG$projects), function(key) {
    config <- CONFIG$projects[[key]]
    
    tryCatch({
      result <- process_slack_project(config, key)
      
      result$channel_metrics <- enrich_channel_metrics(
        file.path(config$results_dir, paste0(key, "_channel_metrics_monthly.csv")),
        config$channels_xlsx,
        key
      )
      
      result$user_metrics <- enrich_user_metrics(
        file.path(config$results_dir, paste0(key, "_user_metrics_monthly.csv")),
        config$users_xlsx,
        key
      )
      
      result
      
    }, error = function(e) {
      warning("❌ Erro ao processar ", key, ": ", e$message)
      NULL
    })
  }) %>% set_names(names(CONFIG$projects))
  
  cat("\n📦 Carregando dataframes no RStudio...\n")
  
  for (key in names(results)) {
    if (is.null(results[[key]])) next
    
    r <- results[[key]]
    
    assign(paste0(key, "_messages"), r$messages, envir = .GlobalEnv)
    assign(paste0(key, "_users"), r$users, envir = .GlobalEnv)
    assign(paste0(key, "_channels"), r$channels, envir = .GlobalEnv)
    assign(paste0(key, "_channel_metrics"), r$channel_metrics, envir = .GlobalEnv)
    assign(paste0(key, "_user_metrics"), r$user_metrics, envir = .GlobalEnv)
    assign(paste0(key, "_title_metrics_by_channel"), r$title_metrics_by_channel, envir = .GlobalEnv)
    
    cat(sprintf("  ✓ %s_messages (%d linhas)\n", key, nrow(r$messages)))
    cat(sprintf("  ✓ %s_users (%d linhas)\n", key, nrow(r$users)))
    cat(sprintf("  ✓ %s_channels (%d linhas)\n", key, nrow(r$channels)))
    cat(sprintf("  ✓ %s_channel_metrics (%d linhas)\n", key, nrow(r$channel_metrics %||% tibble())))
    cat(sprintf("  ✓ %s_user_metrics (%d linhas)\n", key, nrow(r$user_metrics %||% tibble())))
    cat(sprintf("  ✓ %s_title_metrics_by_channel (%d linhas)\n", key, nrow(r$title_metrics_by_channel %||% tibble())))
  }
  
  
  invisible(results)
}

# -------------------- EXECUÇÃO --------------------
results <- execute_pipeline()
