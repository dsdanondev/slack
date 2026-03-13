# SLACK ANALYSIS - MAIN DASHBOARD

# PACKAGE INSTALLATION AND LOADING

  check_and_install <- function(packages) {
    missing_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
    if (length(missing_packages) > 0) {
      cat("📦 Installing missing packages:", paste(missing_packages, collapse = ", "), "\n")
      for (pkg in missing_packages) {
        tryCatch({
          install.packages(pkg, dependencies = TRUE, quiet = TRUE)
          cat("  ✓ Installed:", pkg, "\n")
        }, error = function(e) {
          cat("  ✗ Failed to install:", pkg, "-", e$message, "\n")
        })
      }
      cat("\n")
    }
  }
  
  required_packages <- c("ggplot2", "dplyr", "tidyr", "scales", "lubridate", 
                         "plotly", "htmlwidgets", "forcats", "stringr", "patchwork")
  
  check_and_install(required_packages)
  
  suppressPackageStartupMessages({
    library(ggplot2); library(dplyr); library(tidyr); library(scales)
    library(lubridate); library(plotly); library(htmlwidgets); library(forcats)
    library(stringr); library(patchwork)
  })


# COLOR CONFIGURATION

  PROJECT_COLORS <- list(alpha = "#003049", beta = "#1982C4")
  
  ROLE_COLORS <- c(
    "backend" = "#264653", "coordinator" = "#287271", "devops" = "#2a9d8f",
    "frontend" = "#8ab17d", "project manager" = "#e9c46a", "qa" = "#bc6b85",
    "tech lead" = "#ec8151", "ux/ui" = "#e36040", "data engineer" = "#f4a261",
    "product owner" = "#9576c9"
  )
  
  MESSAGE_TYPE_COLORS <- c(
    "Total Messages" = "#2C3E50", "Total Private Messages" = "#E74C3C",
    "Total Public Messages" = "#27AE60", "Total User Private Messages" = "#C0392B",
    "Total User Public Messages" = "#229954", "Total Bot Messages" = "#95A5A6"
  )
  
  ROLE_TRANSLATION <- c("coordenador" = "coordinator", "gerente de projetos" = "project manager")


# DATA VALIDATION

  required_dfs <- c("alpha_user_metrics", "beta_user_metrics", "alpha_messages", 
                    "beta_messages", "alpha_channel_metrics", "beta_channel_metrics")
  
  missing_dfs <- required_dfs[!sapply(required_dfs, exists)]
  if (length(missing_dfs) > 0) {
    stop("❌ Error: Missing dataframes: ", paste(missing_dfs, collapse = ", "),
         "\n   Please run the Slack export pipeline first.")
  }

# COMMON DATA PREPARATION
  
  user_metrics_combined <- bind_rows(
    alpha_user_metrics %>% mutate(project = "alpha"),
    beta_user_metrics %>% mutate(project = "beta")
  )
  
  prep_channel_monthly <- function(df, project_key) {
    df %>%
      mutate(
        project = project_key,
        month_date = as.Date(substr(month, 1, 10)),
        channel_key = coalesce(channel_id, channel_name),
        members_who_viewed = coalesce(members_who_viewed, 0L),
        members_who_reacted = coalesce(members_who_reacted, 0L),
        unique_users = coalesce(unique_users, 0L)
      ) %>%
      filter(!is.na(month_date))
  }
  
  alpha_chm <- prep_channel_monthly(alpha_channel_metrics, "alpha")
  beta_chm <- prep_channel_monthly(beta_channel_metrics, "beta")
  chm_both <- bind_rows(alpha_chm, beta_chm)

# 1 - ACTIVE MEMBERS BY MONTH AND ROLE

  active_members_by_role <- user_metrics_combined %>%
    mutate(
      month_date = as.Date(substr(month, 1, 10)),
      user_title_clean = tolower(trimws(user_title))
    ) %>%
    filter(!is.na(user_title_clean), nzchar(user_title_clean), user_title_clean != "na") %>%
    mutate(
      user_title_en = if_else(user_title_clean %in% names(ROLE_TRANSLATION),
                              ROLE_TRANSLATION[user_title_clean], user_title_clean)
    ) %>%
    group_by(month_date, project, user_title_en) %>%
    summarise(member_count = n_distinct(user_id), .groups = "drop")
  
  monthly_totals <- active_members_by_role %>%
    group_by(month_date, project) %>%
    summarise(total_members = sum(member_count), .groups = "drop")
  
  plot_01 <- ggplot(active_members_by_role, aes(x = month_date, y = member_count, fill = user_title_en)) +
    geom_col(position = "stack", width = 25) +
    geom_text(data = monthly_totals, aes(x = month_date, y = total_members, label = total_members),
              inherit.aes = FALSE, vjust = -0.5, size = 3.5, fontface = "bold", color = "gray20") +
    facet_wrap(~ project, ncol = 1, scales = "free_y",
               labeller = labeller(project = function(x) tools::toTitleCase(x))) +
    scale_fill_manual(values = ROLE_COLORS, name = "Role",
                      labels = function(x) tools::toTitleCase(x)) +
    scale_x_date(date_breaks = "1 month", date_labels = "%b %Y", expand = c(0.01, 0.01)) +
    scale_y_continuous(expand = expansion(mult = c(0, 0.1)), breaks = pretty_breaks(n = 6)) +
    labs(x = "Month", y = "Number of Active Members") +
    theme_minimal(base_size = 12) +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1, vjust = 1),
      axis.title = element_text(face = "bold"),
      legend.position = "right",
      legend.title = element_text(face = "bold"),
      panel.grid.major.x = element_blank(),
      panel.grid.minor = element_blank(),
      strip.text = element_text(face = "bold", size = 13),
      strip.background = element_blank()
    )
  
  print(plot_01)
  ggsave("output_01_active_members_by_role.png", plot_01, width = 12, height = 8, dpi = 300, bg = "white")

# 2 - AVERAGE MESSAGES PER MEMBER
  
  build_avg_df <- function(df_proj, proj_key) {
    df_proj %>%
      mutate(month_date = as.Date(substr(month, 1, 10))) %>%
      filter(!is.na(month_date)) %>%
      group_by(month_date) %>%
      summarise(
        avg_total = mean(total_messages, na.rm = TRUE),
        avg_public = mean(public_message_count, na.rm = TRUE),
        avg_private = mean(private_message_count, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(project = proj_key)
  }
  
  df_avg_both <- bind_rows(
    build_avg_df(alpha_user_metrics, "alpha"),
    build_avg_df(beta_user_metrics, "beta")
  )
  
  plot_03 <- ggplot(df_avg_both, aes(x = month_date)) +
    geom_col(aes(y = avg_total, fill = project), position = "identity", alpha = 0.4, width = 25) +
    geom_line(aes(y = avg_public, color = "Public"), linewidth = 1.2) +
    geom_line(aes(y = avg_private, color = "Private"), linewidth = 1.2) +
    facet_wrap(~ project, ncol = 1, scales = "free_y",
               labeller = labeller(project = function(x) tools::toTitleCase(x))) +
    scale_fill_manual(values = c(alpha = PROJECT_COLORS$alpha, beta = PROJECT_COLORS$beta), guide = "none") +
    scale_color_manual(name = "Message Type", values = c("Public" = "#27AE60", "Private" = "#E74C3C")) +
    scale_x_date(date_breaks = "1 month", date_labels = "%b %Y", expand = c(0.01, 0.01)) +
    scale_y_continuous(expand = expansion(mult = c(0, 0.05))) +
    labs(x = "Month", y = "Avg messages per member") +
    theme_minimal(base_size = 12) +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1, vjust = 1),
      axis.title = element_text(face = "bold"),
      legend.position = "right",
      legend.title = element_text(face = "bold"),
      strip.text = element_text(face = "bold", size = 13),
      strip.background = element_blank()
    )
  
  print(plot_03)
  ggsave("output_03_avg_messages_per_member.png", plot_03, width = 12, height = 8, dpi = 300, bg = "white")

  # 3 - COMMUNICATION PREFERENCE BY ROLE (DUMBBELL)
  
  roles_to_exclude <- list(
    alpha = c(),
    beta = c()
  )
  
  roles_to_exclude <- lapply(roles_to_exclude, tolower)
  
  role_comm_pref <- user_metrics_combined %>%
    mutate(
      month_date = as.Date(substr(month, 1, 10)),
      role_clean = tolower(trimws(user_title)),
      role_en = if_else(role_clean %in% names(ROLE_TRANSLATION), ROLE_TRANSLATION[role_clean], role_clean)
    ) %>%
    filter(!is.na(month_date), !is.na(role_en), nzchar(role_en), role_en != "na") %>%
    group_by(project, month_date, role_en) %>%
    summarise(
      public_total = sum(public_message_count, na.rm = TRUE),
      private_total = sum(private_message_count, na.rm = TRUE),
      members = n_distinct(user_id),
      .groups = "drop"
    ) %>%
    filter(members > 0) %>%
    mutate(avg_public_month = public_total / members, avg_private_month = private_total / members) %>%
    group_by(project, role_en) %>%
    summarise(
      avg_public = mean(avg_public_month, na.rm = TRUE),
      avg_private = mean(avg_private_month, na.rm = TRUE),
      .groups = "drop"
    )

  role_comm_pref <- role_comm_pref %>%
    mutate(role_en_lower = tolower(role_en)) %>%
    filter(
      !(project == "alpha" & role_en_lower %in% roles_to_exclude$alpha) &
        !(project == "beta" & role_en_lower %in% roles_to_exclude$beta)
    ) %>%
    select(-role_en_lower)
  
  role_comm_pref <- role_comm_pref %>%
    mutate(role_en = factor(role_en, levels = intersect(names(ROLE_COLORS), unique(role_en))))
  
  dumbbell_segments <- role_comm_pref %>%
    transmute(project, role_en, x0 = avg_private, x1 = avg_public)
  
  dumbbell_points <- role_comm_pref %>%
    select(project, role_en, avg_public, avg_private) %>%
    pivot_longer(c(avg_public, avg_private), names_to = "visibility", values_to = "value") %>%
    mutate(visibility = recode(visibility, avg_public = "Public", avg_private = "Private"))
  
  n_projects <- length(unique(role_comm_pref$project))
  
  plot_04 <- ggplot() +
    geom_segment(data = dumbbell_segments, 
                 aes(y = role_en, yend = role_en, x = x0, xend = x1),
                 color = "gray65", linewidth = 1) +
    geom_point(data = dumbbell_points, 
               aes(x = value, y = role_en, color = visibility), size = 3) +
    facet_wrap(~ project, ncol = n_projects, 
               labeller = labeller(project = function(x) tools::toTitleCase(x)),
               scales = "free_x") + 
    scale_color_manual(name = "Visibility", 
                       values = c("Public" = "#27AE60", "Private" = "#E74C3C"),
                       limits = c("Private","Public")) +
    scale_x_continuous(labels = label_number(accuracy = 1)) +
    labs(x = "Avg messages per member / month", y = "Role") +
    theme_minimal(base_size = 12) +
    theme(
      legend.position = "right",
      legend.title = element_text(face = "bold"),
      axis.title = element_text(face = "bold"),
      axis.title.y = element_text(margin = margin(r = 10)),
      axis.text.y = element_text(size = 10), 
      strip.text = element_text(face = "bold", size = 13),
      strip.background = element_blank(),
      panel.spacing = unit(1.5, "lines") 
    )
  
  print(plot_04)

  width_per_project <- 7
  total_width <- width_per_project * n_projects
  
  ggsave("output_04_comm_preference_dumbbell.png", plot_04, 
         width = total_width, height = 7, dpi = 300, bg = "white")


# 4 - ACTIVE CHANNELS BY MONTH
  
  count_active_channels <- function(df, project_key) {
    df %>%
      mutate(month_date = as.Date(substr(month, 1, 10)), channel_key = coalesce(channel_id, channel_name)) %>%
      filter(!is.na(month_date), is_archived == FALSE | is.na(is_archived)) %>%
      group_by(month_date) %>%
      summarise(active_channels = n_distinct(channel_key), .groups = "drop") %>%
      mutate(project = project_key)
  }
  
  active_both <- bind_rows(
    count_active_channels(alpha_channel_metrics, "alpha"),
    count_active_channels(beta_channel_metrics, "beta")
  )
  
  plot_05 <- ggplot(active_both, aes(x = month_date, y = active_channels, fill = project)) +
    geom_col(position = position_dodge2(width = 25, preserve = "single", padding = 0.1), width = 25) +
    geom_text(aes(label = active_channels),
              position = position_dodge2(width = 25, preserve = "single", padding = 0.1),
              vjust = -0.5, size = 3.5, fontface = "bold", color = "gray20") +
    scale_fill_manual(values = c(alpha = PROJECT_COLORS$alpha, beta = PROJECT_COLORS$beta),
                      name = "Project", labels = function(x) tools::toTitleCase(x)) +
    scale_x_date(date_breaks = "1 month", date_labels = "%b %Y", expand = c(0.01, 0.01)) +
    scale_y_continuous(expand = expansion(mult = c(0, 0.12)), breaks = pretty_breaks(n = 6)) +
    labs(x = "Month", y = "Active Channels (per month)") +
    theme_minimal(base_size = 12) +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1, vjust = 1),
      axis.title = element_text(face = "bold"),
      legend.position = "right",
      legend.title = element_text(face = "bold"),
      panel.grid.major.x = element_blank(),
      panel.grid.minor = element_blank()
    )
  
  print(plot_05)
  ggsave("output_05_active_channels_by_month.png", plot_05, width = 12, height = 8, dpi = 300, bg = "white")


# 5 - CHANNEL LIFECYCLE (TOP 20 CHANNELS)

  top20_per_project <- chm_both %>%
    group_by(project, channel_name) %>%
    summarise(total_msgs = sum(message_count, na.rm = TRUE), .groups = "drop") %>%
    group_by(project) %>%
    slice_max(total_msgs, n = 20, with_ties = FALSE) %>%
    ungroup()
  
  life_df <- chm_both %>% semi_join(top20_per_project, by = c("project", "channel_name"))
  
  plot_lifecycle <- function(proj) {
    ggplot(filter(life_df, project == proj),
           aes(x = month_date, y = message_count, color = channel_name, group = channel_name)) +
      geom_line(linewidth = 0.9, alpha = 0.95) +
      scale_x_date(date_breaks = "1 month", date_labels = "%b %y", expand = c(0.01,0.01)) +
      scale_y_continuous(labels = label_number(accuracy = 1)) +
      guides(color = guide_legend(title = "Channel", ncol = 1, override.aes = list(linewidth = 3))) +
      labs(title = tools::toTitleCase(proj), x = NULL, y = "Messages per month") +
      theme_minimal(base_size = 12) +
      theme(
        legend.position = "right",
        legend.title = element_text(face = "bold"),
        plot.title = element_text(hjust = 0.5, face = "bold"),
        axis.text.x = element_text(angle = 45, hjust = 1, vjust = 1),
        axis.title = element_text(face = "bold")
      )
  }
  
  plot_06 <- plot_lifecycle("alpha") | plot_lifecycle("beta")
  print(plot_06)
  ggsave("output_06_lifecycle_top20.png", plot_06, width = 16, height = 8, dpi = 300, bg = "white")

# 6 - CHANNEL RELEVANCE (POSTERS, VIEWERS, REACTORS)

relevance_monthly_avg <- chm_both %>%
  group_by(project, channel_name, month_date) %>%
  summarise(
    user_msgs  = sum(message_count_humans,    na.rm = TRUE),
    bot_msgs   = sum(message_count_bots,      na.rm = TRUE),
    viewers    = sum(members_who_viewed,      na.rm = TRUE),
    reactors   = sum(members_who_reacted,     na.rm = TRUE),
    threads    = sum(thread_count,            na.rm = TRUE),
    .groups = "drop"
  ) %>%
  group_by(project, channel_name) %>%
  summarise(
    user_msgs_avg  = mean(user_msgs,  na.rm = TRUE),
    bot_msgs_avg   = mean(bot_msgs,   na.rm = TRUE),
    viewers_avg    = mean(viewers,    na.rm = TRUE),
    reactors_avg   = mean(reactors,   na.rm = TRUE),
    threads_avg    = mean(threads,    na.rm = TRUE),
    .groups = "drop"
  ) %>%
  group_by(project) %>%
  # Order by: Views DESC > Reactions DESC > Threads DESC > User Messages DESC
  arrange(desc(viewers_avg), desc(reactors_avg), desc(threads_avg), desc(user_msgs_avg)) %>%
  slice_head(n = 16) %>%  # top 20 per project
  ungroup()

plot_relevance_avg <- function(proj) {
  df <- relevance_monthly_avg %>%
    filter(project == proj) %>%
    mutate(
      # Order channels by the same criteria for consistent visual ordering
      channel_name = forcats::fct_reorder(
        channel_name, 
        viewers_avg + reactors_avg/100 + threads_avg/1000 + user_msgs_avg/10000,
        .desc = FALSE
      )
    ) %>%
    pivot_longer(
      c(user_msgs_avg, bot_msgs_avg, viewers_avg, reactors_avg, threads_avg),
      names_to = "metric", 
      values_to = "value"
    ) %>%
    mutate(
      metric = dplyr::recode(
        metric,
        user_msgs_avg  = "User Messages",
        bot_msgs_avg   = "Bot Messages",
        viewers_avg    = "User Views",
        reactors_avg   = "User Reactions",
        threads_avg    = "Threads"
      ),
      # Set factor order for legend
      metric = factor(
        metric,
        levels = c(
          "User Views",
          "User Reactions",
          "Threads",
          "User Messages",
          "Bot Messages"
        )
      )
    )
  
  ggplot(df, aes(x = channel_name, y = value, fill = metric)) +
    geom_col(position = "dodge", width = 0.7) +
    coord_flip() +
    scale_y_continuous(
      trans = "log1p",  # log(1 + x) transformation - safe for zeros
      breaks = c(0, 1, 5, 10, 25, 50, 100, 250, 500, 1000),
      labels = function(x) ifelse(x == 0, "0", as.character(x))
    ) +
    scale_fill_manual(
      values = c(
        "User Views" = "#264653",      # Blue
        "User Reactions" = "#2a9d8f",  # Green
        "Threads" = "#e9c46a",         # Orange
        "User Messages" = "#f4a261",   # Gray
        "Bot Messages" = "#ef476f"     # Light gray
      ),
      name = NULL
    ) +
    labs(
      title = paste0(tools::toTitleCase(proj), ""),
      x = "Channel", 
      y = "Monthly average (log scale)",
    ) +
    theme_minimal(base_size = 12) +
    theme(
      legend.position = "bottom",
      legend.title = element_text(face = "bold"),
      plot.title = element_text(hjust = 0.5, face = "bold"),
      axis.title = element_text(face = "bold"),
      plot.caption = element_text(hjust = 0.5, face = "italic", size = 9, color = "gray40")
    )
}

# Generate plots with shared legend
p2 <- plot_relevance_avg("alpha") | plot_relevance_avg("beta")
p2 <- p2 + plot_layout(guides = "collect") & theme(legend.position = "bottom")
print(p2)
ggsave("p2_relevance.png", p2, width = 16, height = 9, dpi = 300, bg = "white")


# 7 - HUMANS VS BOTS (MESSAGE SHARE)
  
  hb_share <- chm_both %>%
    group_by(project, month_date) %>%
    summarise(
      Humans = sum(message_count_humans, na.rm = TRUE),
      Bots = sum(message_count_bots, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    pivot_longer(c(Humans, Bots), names_to = "User", values_to = "count") %>%
    group_by(project, month_date) %>%
    mutate(share = if_else(sum(count) > 0, count / sum(count), 0)) %>%
    ungroup()
  
  plot_hb <- function(proj) {
    ggplot(filter(hb_share, project == proj), aes(x = month_date, y = share, fill = User)) +
      geom_col(position = "fill", width = 25) +
      scale_y_continuous(labels = percent_format(accuracy = 1)) +
      scale_x_date(date_breaks = "1 month", date_labels = "%b %y", expand = c(0.01,0.01)) +
      scale_fill_manual(name = "User", values = c("Humans" = "#F4A3C2", "Bots" = "#4A4A4A"),
                        limits = c("Humans","Bots")) +
      labs(title = tools::toTitleCase(proj), x = NULL, y = "Share of messages") +
      theme_minimal(base_size = 12) +
      theme(
        legend.position = "right",
        legend.title = element_text(face = "bold"),
        plot.title = element_text(hjust = 0.5, face = "bold"),
        axis.title = element_text(face = "bold"),
        axis.text.x = element_text(angle = 45, hjust = 1, vjust = 1)
      )
  }
  
  plot_08 <- (plot_hb("alpha") | plot_hb("beta")) + plot_layout(guides = "collect")
  print(plot_08)
  ggsave("output_08_humans_vs_bots.png", plot_08, width = 16, height = 7, dpi = 300, bg = "white")

# 8 - THREADS, REPLIES & RESPONSE RATE

  resp_df <- chm_both %>%
    group_by(project, channel_name) %>%
    summarise(
      threads = sum(thread_count, na.rm = TRUE),
      replies = sum(reply_count, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(response_rate = if_else(threads > 0, replies / threads, NA_real_))
  
  plot_threads_replies <- function(proj) {
    base <- filter(resp_df, project == proj)
    top_threads <- base %>% slice_max(threads, n = 15, with_ties = FALSE)
    top_replies <- base %>% slice_max(replies, n = 15, with_ties = FALSE)
    top_rate <- base %>% filter(threads >= 5) %>% slice_max(response_rate, n = 15, with_ties = FALSE)
    
    p_threads <- ggplot(top_threads, aes(x = reorder(channel_name, threads), y = threads)) +
      geom_col(fill = PROJECT_COLORS[[proj]]) + coord_flip() +
      labs(title = "Threads", x = NULL, y = "Total") +
      theme_minimal(base_size = 11) +
      theme(plot.title = element_text(hjust = 0.5, face = "bold"))
    
    p_replies <- ggplot(top_replies, aes(x = reorder(channel_name, replies), y = replies)) +
      geom_col(fill = "#27AE60") + coord_flip() +
      labs(title = "Replies", x = NULL, y = "Total") +
      theme_minimal(base_size = 11) +
      theme(plot.title = element_text(hjust = 0.5, face = "bold"))
    
    p_rate <- ggplot(top_rate, aes(x = reorder(channel_name, response_rate), y = response_rate)) +
      geom_col(fill = "#BC6B85") + coord_flip() +
      scale_y_continuous(labels = percent_format(accuracy = 1)) +
      labs(title = "Response rate (threads ≥ 5)", x = NULL, y = "%") +
      theme_minimal(base_size = 11) +
      theme(plot.title = element_text(hjust = 0.5, face = "bold"))
    
    (p_threads | p_replies | p_rate) +
      plot_annotation(title = tools::toTitleCase(proj)) &
      theme(plot.title = element_text(hjust = 0.5, face = "bold"), axis.title = element_text(face = "bold"))
  }
  
  plot_09 <- plot_threads_replies("alpha") / plot_threads_replies("beta")
  print(plot_09)
  ggsave("output_09_threads_replies_rate.png", plot_09, width = 16, height = 10, dpi = 300, bg = "white")

# 9 - ROLE × CHANNEL HEATMAP
  
  role_channel_monthly <- function(messages_df, proj) {
    messages_df %>%
      filter(is_human, !is.na(user_title), nzchar(user_title)) %>%
      mutate(
        month_date = floor_date(as.Date(timestamp_local), "month"),
        role = tolower(trimws(user_title))
      ) %>%
      group_by(month_date, channel_name, role) %>%
      summarise(msgs = n(), .groups = "drop") %>%
      group_by(channel_name, role) %>%
      summarise(avg_msgs_per_month = mean(msgs, na.rm = TRUE), .groups = "drop") %>%
      mutate(project = proj)
  }
  
  role_ch_both <- bind_rows(
    role_channel_monthly(alpha_messages, "alpha"),
    role_channel_monthly(beta_messages, "beta")
  )
  
  # Limit to top 15 channels per project
  limit_top <- function(df) {
    keep <- df %>%
      group_by(project, channel_name) %>%
      summarise(total = sum(avg_msgs_per_month, na.rm = TRUE), .groups = "drop") %>%
      group_by(project) %>%
      slice_max(total, n = 15, with_ties = FALSE)
    df %>% semi_join(keep, by = c("project", "channel_name"))
  }
  
  role_ch_both <- limit_top(role_ch_both)
  
  plot_role_ch <- function(proj) {
    ggplot(filter(role_ch_both, project == proj),
           aes(x = channel_name, y = role, fill = avg_msgs_per_month)) +
      geom_tile(color = "white") +
      scale_fill_gradient(low = "#E6F5FF", high = "#084B83", name = "Avg msgs / month") +
      labs(title = tools::toTitleCase(proj), x = "Channel", y = "Role") +
      theme_minimal(base_size = 12) +
      theme(
        legend.position = "right",
        legend.title = element_text(face = "bold"),
        plot.title = element_text(hjust = 0.5, face = "bold"),
        axis.text.x = element_text(angle = 45, hjust = 1, vjust = 1)
      )
  }
  
  plot_12 <- plot_role_ch("alpha") | plot_role_ch("beta")
  print(plot_12)
  ggsave("output_12_role_participation_heatmap.png", plot_12, width = 16, height = 8, dpi = 300, bg = "white")


# Text Mining Analysis ══════════════════════════════════════════════════════

text_mining_packages <- c("tm", "wordcloud", "topicmodels", "tidytext", "SnowballC", "RColorBrewer", "stopwords")
check_and_install(text_mining_packages)

suppressPackageStartupMessages({
  library(tm); library(wordcloud); library(topicmodels)
  library(tidytext); library(SnowballC); library(RColorBrewer); library(stopwords)
})

word_replacements <- list(
)

# Custom stop words
custom_stopwords <- c(
  stopwords("en"),
  stopwords("pt"),
  "http", "https", "www", "com", "gif", "jpg", "png", "tco", "rt",
  "amp", "pra", "pro", "tipo", "ok", "ola", "olá",
  "ta", "tá", "ai", "aí", "eh", "é", "ser", "ter",
  "vai", "vou", "tava", "tive", "aqui", "ali", "la", "lá",
  "vc", "vcs", "cês", "cê", "pra", "pro", "q", "pq",
  "kkk", "hahaha", "rs", "obg", "vlw", "tmj", "nda", "bom", "dia", 
  "py", "line", "gt","subteams01edmhtnnq", "boa", "pessoal", "pode", 
  "tarde", "sim", "agora", "certo", "deu", "fazer", "posso", "alguém", 
  "acho", "sim", "dar", "favor", "ainda", "ver", "gente", "guys", "nao", 
  "amigo","file", "tudo", "todos", "sobre", "então", "show", "algum",
  "assim","podem", "qualquer","alguma", "podemos",
  as.character(0:100)
)

clean_text <- function(text) {
  # Passo 1: Limpeza básica
  cleaned_text <- text %>%
    tolower() %>%
    str_remove_all("http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+") %>%
    str_remove_all("\\S+@\\S+") %>%
    str_remove_all("@\\w+") %>%
    str_remove_all("#\\w+") %>%
    str_remove_all("[^[:alnum:][:space:]]") %>%
    str_squish()

  for (old_word in names(word_replacements)) {
    new_word <- word_replacements[[old_word]]
    # Usar word boundaries (\\b) para substituir apenas palavras completas
    pattern <- paste0("\\b", old_word, "\\b")
    cleaned_text <- str_replace_all(cleaned_text, pattern, new_word)
  }
  
  return(cleaned_text)
}

# Get top 15 channels per project
top_channels_alpha <- alpha_messages %>%
  filter(!is.na(channel_name)) %>%
  count(channel_name, sort = TRUE) %>%
  slice_head(n = 20) %>%
  pull(channel_name)

top_channels_beta <- beta_messages %>%
  filter(!is.na(channel_name)) %>%
  count(channel_name, sort = TRUE) %>%
  slice_head(n = 20) %>%
  pull(channel_name)

# Prepare human text (Sections 14-15, 17-19)
prepare_human_text <- function(messages_df, top_channels, project_key) {
  messages_df %>%
    filter(channel_name %in% top_channels, is_human == TRUE, !is.na(text), nchar(text) > 0) %>%
    mutate(text_clean = clean_text(text), project = project_key) %>%
    filter(nchar(text_clean) > 0) %>%
    select(project, channel_name, user_id, text_clean, timestamp_local)
}

alpha_human_text <- prepare_human_text(alpha_messages, top_channels_alpha, "alpha")
beta_human_text <- prepare_human_text(beta_messages, top_channels_beta, "beta")

# Prepare all text (Section 16)
prepare_all_text <- function(messages_df, top_channels, project_key) {
  messages_df %>%
    filter(channel_name %in% top_channels, !is.na(text), nchar(text) > 0) %>%
    mutate(text_clean = clean_text(text), project = project_key) %>%
    filter(nchar(text_clean) > 0) %>%
    select(project, channel_name, text_clean)
}

alpha_all_text <- prepare_all_text(alpha_messages, top_channels_alpha, "alpha")
beta_all_text <- prepare_all_text(beta_messages, top_channels_beta, "beta")


# MOST FREQUENT WORDS

get_word_frequencies <- function(text_df) {
  text_df %>%
    unnest_tokens(word, text_clean) %>%
    filter(!word %in% custom_stopwords, nchar(word) > 2, !str_detect(word, "^\\d+$")) %>%
    count(project, word, sort = TRUE) %>%
    group_by(project) %>%
    slice_head(n = 30) %>%
    ungroup()
}

word_freq_both <- bind_rows(
  get_word_frequencies(alpha_human_text),
  get_word_frequencies(beta_human_text)
)

for (proj in c("alpha", "beta")) {
  cat(sprintf("\n%s:\n", tools::toTitleCase(proj)))
  top_words <- word_freq_both %>% 
    filter(project == proj) %>% 
    slice_head(n = 10)
  
  for (i in 1:nrow(top_words)) {
    cat(sprintf("   %2d. %s (%d)\n", i, top_words$word[i], top_words$n[i]))
  }
}

plot_word_freq <- function(proj) {
  df <- word_freq_both %>% filter(project == proj) %>% mutate(word = reorder(word, n))
  ggplot(df, aes(x = word, y = n, fill = n)) +
    geom_col() + coord_flip() +
    scale_fill_gradient(low = "#E6F5FF", high = "#003049", guide = "none") +
    labs(title = tools::toTitleCase(proj), x = NULL, y = "Frequency") +
    theme_minimal(base_size = 12) +
    theme(plot.title = element_text(hjust = 0.5, face = "bold"),
          axis.title = element_text(face = "bold"),
          panel.grid.major.y = element_blank())
}

plot_14 <- plot_word_freq("alpha") | plot_word_freq("beta")
print(plot_14)
ggsave("output_14_most_frequent_words.png", plot_14, width = 16, height = 10, dpi = 300, bg = "white")

# WORD CLOUDS

generate_wordcloud_data <- function(text_df, min_freq = 5) {
  text_df %>%
    unnest_tokens(word, text_clean) %>%
    filter(!word %in% custom_stopwords, nchar(word) > 2, !str_detect(word, "^\\d+$")) %>%
    count(word, sort = TRUE) %>%
    filter(n >= min_freq)
}

alpha_wc_data <- generate_wordcloud_data(alpha_human_text)
beta_wc_data <- generate_wordcloud_data(beta_human_text)

create_wordcloud_plot <- function(wc_data, project_name, color_palette) {
  par(mar = c(0, 0, 2, 0))
  set.seed(123)
  wordcloud(words = wc_data$word, freq = wc_data$n, min.freq = 1, max.words = 100,
            random.order = FALSE, rot.per = 0.35, colors = color_palette, scale = c(4, 0.5))
  title(main = paste(tools::toTitleCase(project_name), "- Word Cloud"), cex.main = 1.5, font.main = 2)
}

png("output_15_wordclouds_combined.png", width = 2400, height = 1200, res = 150)
par(mfrow = c(1, 2))
create_wordcloud_plot(alpha_wc_data, "alpha", brewer.pal(8, "Blues")[3:8])
create_wordcloud_plot(beta_wc_data, "beta", brewer.pal(8, "PuBu")[3:8])
dev.off()

# TOPIC MODELING (LDA)

prepare_dtm_by_channel <- function(text_df) {
  channel_texts <- text_df %>%
    group_by(channel_name) %>%
    summarise(text = paste(text_clean, collapse = " "), .groups = "drop")
  
  corpus <- Corpus(VectorSource(channel_texts$text))
  dtm <- DocumentTermMatrix(corpus, control = list(
    tolower = TRUE, removePunctuation = TRUE, removeNumbers = TRUE,
    stopwords = custom_stopwords, wordLengths = c(3, Inf), weighting = weightTf
  ))
  dtm <- removeSparseTerms(dtm, 0.95)
  row_totals <- slam::row_sums(dtm)
  dtm <- dtm[row_totals > 0, ]
  list(dtm = dtm, channels = channel_texts$channel_name[row_totals > 0])
}

run_lda_analysis <- function(text_df, n_topics = 5) {
  dtm_data <- prepare_dtm_by_channel(text_df)
  if (nrow(dtm_data$dtm) < 2) return(NULL)
  lda_model <- LDA(dtm_data$dtm, k = n_topics, control = list(seed = 123))
  topics <- tidy(lda_model, matrix = "beta") %>%
    group_by(topic) %>% slice_max(beta, n = 10) %>% ungroup()
  list(topics = topics)
}

plot_topics <- function(lda_result, project_name) {
  if (is.null(lda_result)) return(NULL)
  lda_result$topics %>%
    mutate(term = reorder_within(term, beta, topic)) %>%
    ggplot(aes(x = beta, y = term, fill = factor(topic))) +
    geom_col(show.legend = FALSE) +
    facet_wrap(~ topic, scales = "free_y", ncol = 2) +
    scale_y_reordered() +
    scale_fill_brewer(palette = "Set3") +
    labs(title = paste(tools::toTitleCase(project_name), "- Topics"),
         x = "Topic probability (beta)", y = NULL) +
    theme_minimal(base_size = 11) +
    theme(plot.title = element_text(hjust = 0.5, face = "bold"),
          axis.title = element_text(face = "bold"),
          strip.text = element_text(face = "bold"))
}

alpha_lda <- run_lda_analysis(alpha_all_text, n_topics = 5)
beta_lda <- run_lda_analysis(beta_all_text, n_topics = 5)

if (!is.null(alpha_lda)) {
  plot_16a <- plot_topics(alpha_lda, "alpha")
  print(plot_16a)
  ggsave("output_16a_topics_alpha.png", plot_16a, width = 12, height = 10, dpi = 300, bg = "white")
}

if (!is.null(beta_lda)) {
  plot_16b <- plot_topics(beta_lda, "beta")
  print(plot_16b)
  ggsave("output_16b_topics_beta.png", plot_16b, width = 12, height = 10, dpi = 300, bg = "white")
}
