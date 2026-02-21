## ============================================================
## link_evictions_infousa_utils.R
## ============================================================
## Utility functions for eviction defendant <-> InfoUSA person linkage.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
})

parse_cli_args <- function(args) {
  out <- list()
  if (length(args) == 0L) return(out)
  for (arg in args) {
    if (!startsWith(arg, "--")) next
    arg <- sub("^--", "", arg)
    parts <- strsplit(arg, "=", fixed = TRUE)[[1L]]
    key <- gsub("-", "_", parts[1L])
    val <- if (length(parts) >= 2L) paste(parts[-1L], collapse = "=") else "TRUE"
    out[[key]] <- val
  }
  out
}

coalesce_chr <- function(x, y = "") {
  x <- as.character(x)
  x[is.na(x)] <- y
  x
}

normalize_name <- function(x) {
  x <- coalesce_chr(x, "")
  x <- toupper(x)
  x <- str_replace_all(x, "[’']", "")
  x <- str_replace_all(x, "[^A-Z0-9 ]", " ")
  x <- str_replace_all(x, "\\b(JR|SR|II|III|IV|MR|MRS|MS|DR)\\b", " ")
  x <- str_squish(x)
  x[x == ""] <- NA_character_
  x
}

normalize_address_key <- function(x) {
  x <- coalesce_chr(x, "")
  x <- toupper(x)
  x <- str_replace_all(x, "[^A-Z0-9 ]", " ")
  x <- str_squish(x)
  x[x == ""] <- NA_character_
  x
}

normalize_gender <- function(x) {
  x <- toupper(str_squish(coalesce_chr(x, "")))
  out <- fifelse(
    x %chin% c("M", "MALE"), "M",
    fifelse(x %chin% c("F", "FEMALE"), "F", NA_character_)
  )
  out
}

make_evict_person_id <- function(evict_id, first_name, last_name) {
  paste(
    coalesce_chr(evict_id, ""),
    coalesce_chr(first_name, ""),
    coalesce_chr(last_name, ""),
    sep = "|"
  )
}

parse_int_vector <- function(x, default = c(0L, -1L)) {
  if (is.null(x)) return(as.integer(default))

  if (is.numeric(x)) {
    vals <- as.integer(x)
  } else {
    x <- gsub("\\[|\\]|\\s", "", as.character(x))
    if (!nzchar(x)) return(as.integer(default))
    vals <- as.integer(strsplit(x, ",", fixed = TRUE)[[1L]])
  }

  vals <- vals[!is.na(vals)]
  if (length(vals) == 0L) return(as.integer(default))

  # Interpret two-element ascending vectors as an inclusive range.
  if (length(vals) == 2L && vals[1] <= vals[2]) {
    return(as.integer(seq.int(vals[1], vals[2])))
  }

  unique(vals)
}

load_nickname_dict <- function(path, log_file = NULL) {
  if (is.null(path) || !nzchar(path) || !file.exists(path)) {
    if (exists("logf", mode = "function")) {
      logf("Nickname dictionary missing: ", coalesce_chr(path, "<none>"),
           " (continuing with empty dictionary)", log_file = log_file)
    }
    return(data.table(variant = character(), canonical = character()))
  }

  dt <- fread(path)
  if (nrow(dt) == 0L) {
    return(data.table(variant = character(), canonical = character()))
  }

  if (!all(c("variant", "canonical") %in% names(dt))) {
    if (ncol(dt) < 2L) {
      stop("Nickname dictionary must have at least 2 columns, got: ", ncol(dt))
    }
    setnames(dt, old = names(dt)[1:2], new = c("variant", "canonical"))
  }

  dt <- dt[, .(
    variant = normalize_name(variant),
    canonical = normalize_name(canonical)
  )]
  dt <- dt[!is.na(variant) & !is.na(canonical)]

  # Make nickname mapping symmetric and include identity rows.
  dt2 <- unique(rbind(
    dt,
    dt[, .(variant = canonical, canonical = variant)],
    dt[, .(variant = canonical, canonical = canonical)],
    dt[, .(variant = variant, canonical = variant)]
  ))

  if (exists("logf", mode = "function")) {
    logf("Loaded nickname dictionary rows: ", nrow(dt2), " from ", path, log_file = log_file)
  }

  dt2
}

is_nickname_match <- function(a, b, nick_dt) {
  if (is.null(nick_dt) || nrow(nick_dt) == 0L) return(rep(FALSE, length(a)))

  a <- normalize_name(a)
  b <- normalize_name(b)
  out <- rep(FALSE, length(a))

  key_dt <- data.table(idx = seq_along(a), variant = a, canonical = b)
  hits <- merge(key_dt, nick_dt, by = c("variant", "canonical"), all = FALSE)
  out[unique(hits$idx)] <- TRUE
  out
}

first_name_score <- function(first_ev, first_inf, nick_dt = NULL) {
  a <- normalize_name(first_ev)
  b <- normalize_name(first_inf)

  out <- rep(NA_real_, length(a))
  both <- !is.na(a) & !is.na(b)
  if (!any(both)) return(out)

  exact <- both & (a == b)
  out[exact] <- 1.0

  rem <- both & is.na(out)
  if (any(rem)) {
    nick <- is_nickname_match(a[rem], b[rem], nick_dt)
    rem_idx <- which(rem)
    nick_idx <- rem_idx[nick]
    out[nick_idx] <- 0.95

    still <- rem
    if (length(nick_idx) > 0L) still[nick_idx] <- FALSE

    if (any(still)) {
      if (requireNamespace("stringdist", quietly = TRUE)) {
        sim <- stringdist::stringsim(a[still], b[still], method = "jw")
      } else {
        ad <- utils::adist(a[still], b[still])
        max_len <- pmax(nchar(a[still]), nchar(b[still]))
        sim <- 1 - (ad / pmax(1, max_len))
      }
      sim <- pmax(0, pmin(1, as.numeric(sim)))
      out[still] <- sim
    }
  }

  out
}

gender_compatible <- function(g_ev, g_inf) {
  g1 <- normalize_gender(g_ev)
  g2 <- normalize_gender(g_inf)

  out <- rep(NA, length(g1))
  both <- !is.na(g1) & !is.na(g2)
  out[both] <- g1[both] == g2[both]
  out
}

derive_bldg_size_bin <- function(total_units) {
  u <- suppressWarnings(as.numeric(total_units))
  out <- fifelse(
    is.na(u), "unknown_size",
    fifelse(u <= 1, "1",
      fifelse(u <= 2, "2",
        fifelse(u <= 4, "3-4",
          fifelse(u <= 9, "5-9",
            fifelse(u <= 19, "10-19",
              fifelse(u <= 49, "20-49", "50+")
            )
          )
        )
      )
    )
  )
  out
}

schema_version_tag <- function() {
  "v0_scaffold_2026-02-19"
}

collapse_person_nums <- function(x) {
  x <- unique(as.character(x))
  x <- x[!is.na(x) & nzchar(x)]
  if (length(x) == 0L) return(NA_character_)
  paste(sort(x), collapse = ",")
}
