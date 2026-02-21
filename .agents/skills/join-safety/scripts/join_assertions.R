# Minimal helpers for join safety checks (drop-in)
suppressPackageStartupMessages(library(data.table))

assert_unique <- function(dt, keys, label = "table") {
  stopifnot(is.data.table(dt))
  dup_n <- dt[, .N, by = keys][N > 1L, .N]
  if (length(dup_n) > 0L) {
    stop(sprintf("[%s] Expected unique keys but found duplicates on: %s",
                 label, paste(keys, collapse = ",")))
  }
  invisible(TRUE)
}

summarize_join <- function(left, right, keys, label = "join") {
  stopifnot(is.data.table(left), is.data.table(right))
  left_keys <- unique(left[, ..keys])
  right_keys <- unique(right[, ..keys])
  m_left <- nrow(left_keys)
  m_right <- nrow(right_keys)
  merged <- merge(left_keys, right_keys, by = keys, all = FALSE)
  m_both <- nrow(merged)
  list(
    label = label,
    keys = keys,
    n_left = nrow(left),
    n_right = nrow(right),
    n_left_keys = m_left,
    n_right_keys = m_right,
    n_keys_intersection = m_both,
    share_left_keys_matched = if (m_left == 0) NA_real_ else m_both / m_left,
    share_right_keys_matched = if (m_right == 0) NA_real_ else m_both / m_right
  )
}

assert_row_change <- function(n_before, n_after, upper_mult = 1.05, label = "step") {
  if (is.na(n_before) || is.na(n_after)) return(invisible(TRUE))
  if (n_after > upper_mult * n_before) {
    stop(sprintf("[%s] Row count increased too much: %s -> %s (limit x%.3f)",
                 label, n_before, n_after, upper_mult))
  }
  invisible(TRUE)
}
