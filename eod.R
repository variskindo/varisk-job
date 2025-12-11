# ==============================================================================
# INCREMENTAL UPDATE SCRIPT: STOCK DATA TO CLOUDFLARE R2
# Menggunakan Wrangler CLI untuk upload (paling reliable)
# ==============================================================================
# 
# PREREQUISITE:
# 1. Install Node.js
# 2. npm install -g wrangler
# 3. wrangler login (atau set CLOUDFLARE_API_TOKEN)
#
# ENCODING SCHEME:
# ┌─────────────┬────────────────────────────────────────────────────────────┐
# │ Storage     │ Encoding                                                   │
# ├─────────────┼────────────────────────────────────────────────────────────┤
# │ R2 filename │ Minimal: hanya encode karakter URL-unsafe (^ # $ ? & [ ])  │
# │             │ Titik (.) TIDAK di-encode                                  │
# │             │ Contoh: BBCA.JK -> BBCA.JK.json                            │
# │             │         ^JKSE   -> _C_JKSE.json                            │
# ├─────────────┼────────────────────────────────────────────────────────────┤
# │ Firebase    │ Full: encode semua karakter tidak valid (. ^ # $ [ ])      │
# │ RTDB key    │ Contoh: BBCA.JK -> BBCA_JK                                 │
# │             │         ^JKSE   -> _C_JKSE                                 │
# ├─────────────┼────────────────────────────────────────────────────────────┤
# │ Local cache │ Sama dengan R2 filename (jika USE_LOCAL_CACHE = TRUE)      │
# │             │ Lokasi: LOCAL_CACHE_DIR/BBCA.JK.json                       │
# └─────────────┴────────────────────────────────────────────────────────────┘
#
# LOCAL CACHE STRATEGY:
# - Jika USE_LOCAL_CACHE = TRUE:
#   - Setiap upload ke R2, file juga disimpan di local cache
#   - Pada pengecekan, baca dari local cache dulu (CEPAT)
#   - Jika cache miss, fallback ke R2 (lalu sync ke cache)
#   - Menghemat waktu & bandwidth untuk 22k+ symbols
# - Jika USE_LOCAL_CACHE = FALSE:
#   - Langsung baca dari R2 (untuk GitHub Actions / server tanpa local storage)
#
# ==============================================================================

# --- 1. SETUP LIBRARIES ---
required_packages <- c("quantmod", "rugarch", "TTR", "jsonlite", "httr2", 
                       "lubridate", "dplyr")
new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)

suppressPackageStartupMessages({
  library(quantmod)
  library(rugarch)
  library(TTR)
  library(dplyr)
  library(jsonlite)
  library(httr2)
  library(lubridate)
})

# --- 2. CONFIGURATION ---

# ==============================================================================
# LOCAL CACHE TOGGLE
# Set TRUE untuk eksekusi di local machine (iMac, MacBook, dll)
# Set FALSE untuk eksekusi di GitHub Actions atau server tanpa persistent storage
# ==============================================================================
USE_LOCAL_CACHE <- FALSE #as.logical(Sys.getenv("USE_LOCAL_CACHE", "TRUE"))

# Cloudflare R2 Configuration
R2_BUCKET      <- "varisk-kancil"
R2_PUBLIC_URL  <- Sys.getenv("R2_PUBLIC_URL", "https://pub-16889429f5234d72850daf7a0f23502e.r2.dev")

# Path prefix untuk historical data
R2_HIST_PREFIX <- "historical"

# Local cache directory - untuk menyimpan salinan JSON agar tidak perlu download dari R2
# Hanya digunakan jika USE_LOCAL_CACHE = TRUE
LOCAL_CACHE_DIR <- "./cache" #Sys.getenv("LOCAL_CACHE_DIR", "/Users/bennyky/Documents/R/r2_cache/prices")

# Local temp directory untuk files sebelum upload
TEMP_DIR       <- file.path(tempdir(), "r2_upload")

# Firebase RTDB Configuration
RTDB_URL       <- "https://varisk-kancil-default-rtdb.asia-southeast1.firebasedatabase.app"
RTDB_SECRET    <- Sys.getenv("RTDB_SECRET", "PokBlUENFZV0lEypKPy4KiWwgFCV9Jb98IgSneQo")

# Data Configuration
FROM_DATE        <- "2016-01-04"
CALC_BUFFER_DAYS <- 300

# --- 3. HELPER FUNCTIONS ---

# encode_ticker_for_r2: HANYA untuk karakter yang bermasalah di URL/R2
# Karakter aman: . (titik), - (dash), _ (underscore), alfanumerik
# Karakter bermasalah: ^ # $ ? & [ ]
encode_ticker_for_r2 <- function(ticker) {
  if (is.null(ticker) || is.na(ticker)) return(ticker)
  x <- gsub("\\^", "_C_", ticker)   # Caret (^JKSE -> _C_JKSE)
  x <- gsub("#", "_H_", x)          # Hash
  x <- gsub("\\$", "_D_", x)        # Dollar
  x <- gsub("\\?", "_Q_", x)        # Question mark
  x <- gsub("&", "_A_", x)          # Ampersand
  x <- gsub("\\[", "_LB_", x)       # Left bracket
  x <- gsub("\\]", "_RB_", x)       # Right bracket
  return(x)
}

# decode_ticker_from_r2: Mengembalikan dari format R2 ke ticker asli
decode_ticker_from_r2 <- function(encoded) {
  if (is.null(encoded) || is.na(encoded)) return(encoded)
  x <- gsub("_RB_", "]", encoded)
  x <- gsub("_LB_", "[", x)
  x <- gsub("_A_", "&", x)
  x <- gsub("_Q_", "?", x)
  x <- gsub("_D_", "$", x)
  x <- gsub("_H_", "#", x)
  x <- gsub("_C_", "^", x)
  return(x)
}

# encode_ticker_for_rtdb: untuk Firebase RTDB (tidak support "." di key)
encode_ticker_for_rtdb <- function(ticker) {
  if (is.null(ticker) || is.na(ticker)) return(ticker)
  x <- gsub("\\.", "_", ticker)
  x <- gsub("\\^", "_C_", x)
  x <- gsub("#", "_H_", x)
  x <- gsub("\\$", "_D_", x)
  x <- gsub("\\[", "_LB_", x)
  x <- gsub("\\]", "_RB_", x)
  return(x)
}

# decode_ticker_from_rtdb: Mengembalikan dari format RTDB ke ticker asli
decode_ticker_from_rtdb <- function(encoded) {
  if (is.null(encoded) || is.na(encoded)) return(encoded)
  x <- gsub("_RB_", "]", encoded)
  x <- gsub("_LB_", "[", x)
  x <- gsub("_D_", "$", x)
  x <- gsub("_H_", "#", x)
  x <- gsub("_C_", "^", x)
  x <- gsub("_", ".", x)
  return(x)
}

format_number <- function(x, digits = 6) {
  if (is.null(x) || is.na(x) || !is.finite(x)) return(NULL)
  return(round(x, digits))
}

# ==============================================================================
# LOCAL CACHE OPERATIONS
# ==============================================================================

#' Ensure local cache directory exists
ensure_cache_dir <- function() {
  if (!USE_LOCAL_CACHE) return(invisible(NULL))
  
  if (!dir.exists(LOCAL_CACHE_DIR)) {
    dir.create(LOCAL_CACHE_DIR, recursive = TRUE)
    message(sprintf("   Created local cache directory: %s", LOCAL_CACHE_DIR))
  }
}

#' Get local cache file path for a ticker
get_cache_path <- function(r2_encoded_ticker) {
  file.path(LOCAL_CACHE_DIR, paste0(r2_encoded_ticker, ".json"))
}

#' Read data from local cache
#' @return data list or NULL if not found or cache disabled
read_from_cache <- function(r2_encoded_ticker) {
  if (!USE_LOCAL_CACHE) return(NULL)
  
  cache_path <- get_cache_path(r2_encoded_ticker)
  
  if (file.exists(cache_path)) {
    tryCatch({
      data <- fromJSON(cache_path, simplifyVector = FALSE)
      return(data)
    }, error = function(e) {
      message(sprintf("   -> [WARN] Failed to read cache: %s", e$message))
      return(NULL)
    })
  }
  return(NULL)
}

#' Write data to local cache
write_to_cache <- function(data, r2_encoded_ticker) {
  if (!USE_LOCAL_CACHE) return(invisible(FALSE))
  
  tryCatch({
    ensure_cache_dir()
    cache_path <- get_cache_path(r2_encoded_ticker)
    json_content <- toJSON(data, auto_unbox = TRUE, digits = 6, pretty = FALSE)
    writeLines(json_content, cache_path)
    return(TRUE)
  }, error = function(e) {
    message(sprintf("   -> [WARN] Failed to write cache: %s", e$message))
    return(FALSE)
  })
}

# ==============================================================================
# R2 OPERATIONS (menggunakan Wrangler CLI)
# ==============================================================================

#' Check if wrangler is installed
check_wrangler <- function() {
  node_ver <- tryCatch(
    system("node -v", intern = TRUE),
    error = function(e) NA
  )
  
  if (is.na(node_ver) || !grepl("^v20\\.", node_ver)) {
    stop(sprintf("Node.js v20.x diperlukan. Saat ini terdeteksi: %s", node_ver))
  }
  
  res <- tryCatch(
    system("wrangler --version", intern = TRUE),
    error = function(e) character(0)
  )
  
  if (length(res) == 0) {
    stop("Wrangler CLI tidak ditemukan atau gagal dijalankan. Pastikan sudah ter-install dengan benar.")
  }
  
  message("Wrangler OK: ", res)
  invisible(res)
}

#' Upload file to R2 using wrangler AND optionally save to local cache
#' @param data Data list to upload as JSON
#' @param key R2 object key (path in bucket)
#' @param r2_encoded_ticker Encoded ticker for cache filename
upload_to_r2 <- function(data, key, r2_encoded_ticker) {
  tryCatch({
    if (!dir.exists(TEMP_DIR)) {
      dir.create(TEMP_DIR, recursive = TRUE)
    }
    
    # Gunakan ticker asli untuk nama file lokal juga
    temp_file <- file.path(TEMP_DIR, basename(key))
    json_content <- toJSON(data, auto_unbox = TRUE, digits = 6, pretty = FALSE)
    writeLines(json_content, temp_file)
    
    # Upload using wrangler - key sudah dalam format asli (BBCA.JK.json)
    cmd <- sprintf(
      'wrangler r2 object put "%s/%s" --file="%s" --content-type="application/json" --remote',
      R2_BUCKET, key, temp_file
    )
    
    result <- system(cmd, intern = TRUE, ignore.stderr = FALSE)
    exit_code <- attr(result, "status")
    
    unlink(temp_file)
    
    if (is.null(exit_code) || exit_code == 0) {
      message(sprintf("   -> R2 uploaded: %s", key))
      
      # Also save to local cache (if enabled)
      if (USE_LOCAL_CACHE) {
        cache_ok <- write_to_cache(data, r2_encoded_ticker)
        if (cache_ok) {
          message(sprintf("   -> Local cache saved: %s.json", r2_encoded_ticker))
        }
      }
      
      return(TRUE)
    } else {
      message(sprintf("   -> [WARN] Wrangler upload failed (exit %s):\n%s",
                      exit_code, paste(result, collapse = "\n")))
      return(FALSE)
    }
  }, error = function(e) {
    message(sprintf("   -> [ERROR] R2 upload failed: %s", e$message))
    return(FALSE)
  })
}

#' Download JSON from R2 - via Public URL
#' @param key R2 object key - menggunakan ticker asli
download_from_r2 <- function(key) {
  public_url <- sprintf("%s/%s", R2_PUBLIC_URL, key)
  
  tryCatch({
    resp <- request(public_url) %>% 
      req_timeout(30) %>% 
      req_error(is_error = function(resp) FALSE) %>%
      req_perform()
    
    if (resp_status(resp) == 200) {
      content <- resp_body_string(resp)
      data <- fromJSON(content, simplifyVector = FALSE)
      return(data)
    } else if (resp_status(resp) == 404) {
      return(NULL)
    } else {
      message(sprintf("   -> [WARN] R2 download HTTP %d", resp_status(resp)))
      return(NULL)
    }
  }, error = function(e) {
    return(NULL)
  })
}

#' Test R2 connection using wrangler
test_r2_connection <- function(bucket = "varisk-kancil") {
  cmd <- "wrangler r2 bucket list"
  
  out <- tryCatch(
    system(cmd, intern = TRUE),
    error = function(e) {
      message("⚠️ Error menjalankan wrangler: ", e$message)
      return(character(0))
    }
  )
  
  if (length(out) == 0) {
    message("⚠️ Tidak ada output dari 'wrangler r2 bucket list'.")
    return(FALSE)
  }
  
  status <- attr(out, "status")
  if (!is.null(status) && status != 0) {
    message("⚠️ wrangler r2 bucket list gagal dengan status: ", status)
    print(out)
    return(FALSE)
  }
  
  found <- any(grepl(bucket, out, fixed = TRUE))
  
  if (found) {
    message("✅ R2 bucket '", bucket, "' ditemukan. Koneksi OK.")
    return(TRUE)
  } else {
    message("⚠️ Bucket '", bucket, "' TIDAK ditemukan di daftar bucket:")
    print(out)
    return(FALSE)
  }
}

# ==============================================================================
# RTDB OPERATIONS
# ==============================================================================

read_rtdb <- function(path) {
  url <- sprintf("%s/%s.json?auth=%s", RTDB_URL, path, RTDB_SECRET)
  tryCatch({
    resp <- request(url) %>% req_timeout(30) %>% req_perform()
    if (resp_status(resp) == 200) {
      cnt <- resp_body_string(resp)
      if (cnt == "null") return(NULL)
      return(fromJSON(cnt, simplifyVector = FALSE))
    }
    return(NULL)
  }, error = function(e) return(NULL))
}

patch_rtdb <- function(path, data) {
  url <- sprintf("%s/%s.json?auth=%s", RTDB_URL, path, RTDB_SECRET)
  tryCatch({
    resp <- request(url) %>%
      req_method("PATCH") %>%
      req_body_json(data, auto_unbox = TRUE) %>%
      req_timeout(30) %>%
      req_perform()
    return(resp_status(resp) == 200)
  }, error = function(e) return(FALSE))
}

# ==============================================================================
# CALCULATION FUNCTIONS
# ==============================================================================

calculate_metrics <- function(stock_data) {
  if (is.null(stock_data) || nrow(stock_data) < 2) return(NULL)
  
  tryCatch({
    prices <- na.omit(stock_data)
    
    if (nrow(prices) < 2) return(NULL)
    
    ret <- dailyReturn(Ad(prices), type = "log")
    colnames(ret) <- "ret"
    
    n_rows <- nrow(ret)
    
    vol30 <- if (n_rows >= 30) runSD(ret, n = 30) else xts(rep(NA, n_rows), order.by = index(ret))
    vol60 <- if (n_rows >= 60) runSD(ret, n = 60) else xts(rep(NA, n_rows), order.by = index(ret))
    vol90 <- if (n_rows >= 90) runSD(ret, n = 90) else xts(rep(NA, n_rows), order.by = index(ret))
    
    lambda <- 0.94
    ewma_vol <- tryCatch({
      if (n_rows >= 10) sqrt(EMA(ret^2, n = 1 / (1 - lambda), ratio = 1 - lambda)) 
      else xts(rep(NA, n_rows), order.by = index(ret))
    }, error = function(e) xts(rep(NA, n_rows), order.by = index(ret)))
    
    garch_vol <- xts(rep(NA, n_rows), order.by = index(ret))
    if (n_rows >= 100) {
      tryCatch({
        spec <- ugarchspec(
          variance.model = list(model = "sGARCH", garchOrder = c(1, 1)),
          mean.model = list(armaOrder = c(0, 0), include.mean = TRUE),
          distribution.model = "norm"
        )
        fit <- ugarchfit(spec = spec, data = ret, solver = "hybrid")
        garch_vol <- sigma(fit)
      }, error = function(e) {
        message(sprintf("   -> [WARN] GARCH failed: %s", e$message))
      })
    }
    
    final_xts <- merge(prices, ret, vol30, vol60, vol90, ewma_vol, garch_vol)
    names(final_xts) <- c("open", "high", "low", "close", "volume", "adjusted",
                          "return", "vol30", "vol60", "vol90", "ewma", "garch")
    return(final_xts)
  }, error = function(e) {
    message(sprintf("   -> [WARN] Metrics error: %s", e$message))
    
    tryCatch({
      prices <- na.omit(stock_data)
      if (nrow(prices) < 2) return(NULL)
      
      ret <- dailyReturn(Ad(prices), type = "log")
      n_rows <- nrow(ret)
      
      na_col <- xts(rep(NA, n_rows), order.by = index(ret))
      
      final_xts <- merge(prices, ret, na_col, na_col, na_col, na_col, na_col)
      names(final_xts) <- c("open", "high", "low", "close", "volume", "adjusted",
                            "return", "vol30", "vol60", "vol90", "ewma", "garch")
      return(final_xts)
    }, error = function(e2) {
      message(sprintf("   -> [ERROR] Fallback also failed: %s", e2$message))
      return(NULL)
    })
  })
}

# ==============================================================================
# R2 DATA STRUCTURE
# ==============================================================================

df_to_r2_format <- function(symbol, df) {
  currency <- "USD"
  if (grepl("\\.JK$", symbol)) currency <- "IDR"
  else if (grepl("\\.HK$", symbol)) currency <- "HKD"
  else if (grepl("\\.T$", symbol)) currency <- "JPY"
  else if (grepl("\\.L$", symbol)) currency <- "GBP"
  else if (grepl("\\.(PA|F|MI|MC|AS|BR)$", symbol)) currency <- "EUR"
  else if (grepl("\\.TO$", symbol)) currency <- "CAD"
  else if (grepl("\\.AX$", symbol)) currency <- "AUD"
  else if (grepl("\\.(NS|BO)$", symbol)) currency <- "INR"
  else if (grepl("\\.KS$", symbol)) currency <- "KRW"
  else if (grepl("\\.TW$", symbol)) currency <- "TWD"
  else if (grepl("\\.SG$", symbol)) currency <- "SGD"
  else if (grepl("=X$", symbol)) currency <- "FX"
  else if (grepl("-USD$", symbol)) currency <- "CRYPTO"
  
  data_map <- list()
  
  for (i in 1:nrow(df)) {
    row <- df[i, ]
    date_str <- as.character(row$date)
    
    entry <- list(
      o = format_number(row$open, 4),
      h = format_number(row$high, 4),
      l = format_number(row$low, 4),
      c = format_number(row$close, 4),
      v = if (!is.na(row$volume)) as.integer(row$volume) else NULL,
      a = format_number(row$adjusted, 4),
      r = format_number(row$return, 6),
      vol30 = format_number(row$vol30, 6),
      vol60 = format_number(row$vol60, 6),
      vol90 = format_number(row$vol90, 6),
      ewma = format_number(row$ewma, 6),
      garch = format_number(row$garch, 6)
    )
    
    data_map[[date_str]] <- Filter(Negate(is.null), entry)
  }
  
  result <- list(
    symbol = symbol,  # Tetap simpan symbol asli di dalam JSON
    currency = currency,
    updated = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    count = nrow(df),
    first_date = as.character(min(df$date)),
    last_date = as.character(max(df$date)),
    data = data_map
  )
  
  return(result)
}

merge_r2_data <- function(existing, new_df, symbol) {
  if (is.null(existing) || is.null(existing$data)) {
    return(df_to_r2_format(symbol, new_df))
  }
  
  for (i in 1:nrow(new_df)) {
    row <- new_df[i, ]
    date_str <- as.character(row$date)
    
    entry <- list(
      o = format_number(row$open, 4),
      h = format_number(row$high, 4),
      l = format_number(row$low, 4),
      c = format_number(row$close, 4),
      v = if (!is.na(row$volume)) as.integer(row$volume) else NULL,
      a = format_number(row$adjusted, 4),
      r = format_number(row$return, 6),
      vol30 = format_number(row$vol30, 6),
      vol60 = format_number(row$vol60, 6),
      vol90 = format_number(row$vol90, 6),
      ewma = format_number(row$ewma, 6),
      garch = format_number(row$garch, 6)
    )
    
    existing$data[[date_str]] <- Filter(Negate(is.null), entry)
  }
  
  all_dates <- names(existing$data)
  existing$updated <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  existing$count <- length(all_dates)
  existing$first_date <- min(all_dates)
  existing$last_date <- max(all_dates)
  
  return(existing)
}

# ==============================================================================
# PROCESS SINGLE SYMBOL
# ==============================================================================

process_symbol_incremental <- function(symbol, date_range_info, symbol_idx, total_symbols, start_time) {
  # R2 key: encode hanya karakter bermasalah (^, #, $, dll), titik tetap dipertahankan
  # ^JKSE -> _C_JKSE.json, BBCA.JK -> BBCA.JK.json
  r2_encoded <- encode_ticker_for_r2(symbol)
  r2_key <- sprintf("%s/prices/%s.json", R2_HIST_PREFIX, r2_encoded)
  
  # Firebase RTDB key: encode semua termasuk titik (karena tidak support ".")
  rtdb_encoded <- encode_ticker_for_rtdb(symbol)
  
  elapsed_mins <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
  if (symbol_idx > 1) {
    avg_per_sym <- elapsed_mins / (symbol_idx - 1)
    est_remain <- avg_per_sym * (total_symbols - symbol_idx + 1)
    time_msg <- sprintf("Elapsed: %.1f min | Est. remain: %.1f min", elapsed_mins, est_remain)
  } else {
    time_msg <- sprintf("Elapsed: %.1f min", elapsed_mins)
  }
  
  pct <- round((symbol_idx / total_symbols) * 100, 2)
  message(sprintf("\n[%s] %d/%d (%.2f%%) %s", format(Sys.time(), "%H:%M:%S"), symbol_idx, total_symbols, pct, symbol))
  message(sprintf("   %s", time_msg))
  message(sprintf("   -> R2 key: %s", r2_key))
  
  # PRIORITAS: 1) Local cache (jika enabled), 2) R2 (fallback atau primary jika cache disabled)
  existing_data <- NULL
  data_source <- "none"
  
  if (USE_LOCAL_CACHE) {
    existing_data <- read_from_cache(r2_encoded)
    if (!is.null(existing_data)) {
      data_source <- "cache"
      message("   -> Cache hit!")
    }
  }
  
  if (is.null(existing_data)) {
    if (USE_LOCAL_CACHE) {
      message("   -> Cache miss, checking R2...")
    } else {
      message("   -> Checking R2 (cache disabled)...")
    }
    
    existing_data <- download_from_r2(r2_key)
    data_source <- "r2"
    
    # Jika dapat dari R2 dan cache enabled, simpan ke cache untuk next time
    if (!is.null(existing_data) && USE_LOCAL_CACHE) {
      write_to_cache(existing_data, r2_encoded)
      message("   -> Synced R2 -> local cache")
    }
  }
  
  is_new_symbol <- is.null(existing_data)
  
  if (is_new_symbol) {
    message("   -> NEW SYMBOL: Full download from 2016-01-04")
    download_from <- as.Date(FROM_DATE)
    update_cutoff <- as.Date("1900-01-01")
  } else {
    last_data_date <- as.Date(existing_data$last_date)
    
    if (last_data_date >= Sys.Date() - 1) {
      message("   -> Already up to date. Skipping.")
      return(list(success = TRUE, new_records = 0))
    }
    
    download_from <- last_data_date - CALC_BUFFER_DAYS
    update_cutoff <- last_data_date
    message(sprintf("   -> UPDATE: Last date = %s (from %s), buffer from %s", 
                    last_data_date, data_source, download_from))
  }
  
  message(sprintf("   -> Downloading from Yahoo Finance (%s to %s)...", download_from, Sys.Date()))
  
  stock_data <- tryCatch({
    quantmod::getSymbols(symbol, src = "yahoo", from = download_from, to = Sys.Date() + 1, auto.assign = FALSE)
  }, error = function(e) {
    message(sprintf("   -> [ERROR] Download failed: %s", e$message))
    return(NULL)
  })
  
  if (is.null(stock_data) || nrow(stock_data) < 2) {
    message("   -> [WARN] Insufficient data (< 2 rows). Skipping.")
    return(list(success = FALSE, new_records = 0))
  }
  
  message(sprintf("   -> Downloaded %d rows", nrow(stock_data)))
  
  message("   -> Calculating volatility metrics...")
  metrics <- calculate_metrics(stock_data)
  
  if (is.null(metrics)) {
    message("   -> [ERROR] Failed to calculate metrics.")
    return(list(success = FALSE, new_records = 0))
  }
  
  final_df <- data.frame(date = index(metrics), coredata(metrics))
  new_data <- final_df[final_df$date > update_cutoff, ]
  
  if (nrow(new_data) == 0) {
    message("   -> No new data to save.")
    return(list(success = TRUE, new_records = 0))
  }
  
  message(sprintf("   -> Preparing %d new records (%s to %s)...",
                  nrow(new_data), min(new_data$date), max(new_data$date)))
  
  if (is_new_symbol) {
    r2_data <- df_to_r2_format(symbol, final_df)
  } else {
    r2_data <- merge_r2_data(existing_data, new_data, symbol)
  }
  
  upload_success <- upload_to_r2(r2_data, r2_key, r2_encoded)
  
  if (upload_success) {
    message("   -> R2 upload successful!")
    
    # Update marketSnapshots HANYA untuk stock/ETF/funds
    # Skip: FX (=X), Crypto (-USD), Commodities/Futures (=F) - sudah di-handle oleh Delphi exe secara realtime
    is_stock_or_etf <- !grepl("=X$|-USD$|=F$", symbol)
    
    if (is_stock_or_etf) {
      # Ambil data terakhir dari new_data
      latest_row <- tail(new_data, 1)
      
      # Hitung change dan changePercent jika ada data sebelumnya
      prev_close <- NULL
      change <- NULL
      change_pct <- NULL
      
      if (nrow(new_data) >= 2) {
        prev_row <- new_data[nrow(new_data) - 1, ]
        prev_close <- prev_row$close
        if (!is.na(prev_close) && prev_close != 0) {
          change <- latest_row$close - prev_close
          change_pct <- (change / prev_close) * 100
        }
      }
      
      # =====================================================================
      # SPLIT STORAGE STRATEGY:
      # 1. marketSnapshots/current/symbols - LIGHT (hanya close price)
      #    -> Di-load realtime oleh ribuan user, harus ringan
      # 2. marketSnapshots/current/data - FULL (semua metrics)
      #    -> Di-load on-demand saat user butuh detail
      # =====================================================================
      
      # --- 1. LIGHT DATA: symbols (hanya close price) ---
      snap_symbols <- list()
      snap_symbols[[rtdb_encoded]] <- format_number(latest_row$close, 4)
      
      snap_symbols_success <- patch_rtdb("marketSnapshots/current/symbols", snap_symbols)
      
      if (snap_symbols_success) {
        message(sprintf("   -> RTDB symbols updated: %s = %.4f", 
                        rtdb_encoded, latest_row$close))
      }
      
      # --- 2. FULL DATA: data (semua metrics) ---
      snap_full_data <- list(
        c = format_number(latest_row$close, 4),
        o = format_number(latest_row$open, 4),
        h = format_number(latest_row$high, 4),
        l = format_number(latest_row$low, 4),
        v = if (!is.na(latest_row$volume)) as.integer(latest_row$volume) else NULL,
        a = format_number(latest_row$adjusted, 4),
        r = format_number(latest_row$return, 6),
        chg = format_number(change, 4),
        chgPct = format_number(change_pct, 4),
        vol30 = format_number(latest_row$vol30, 6),
        vol60 = format_number(latest_row$vol60, 6),
        vol90 = format_number(latest_row$vol90, 6),
        ewma = format_number(latest_row$ewma, 6),
        garch = format_number(latest_row$garch, 6),
        date = as.character(latest_row$date),
        updatedAt = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
      )
      
      # Remove NULL values
      snap_full_data <- Filter(Negate(is.null), snap_full_data)
      
      snap_data_body <- list()
      snap_data_body[[rtdb_encoded]] <- snap_full_data
      
      snap_data_success <- patch_rtdb("marketSnapshots/current/data", snap_data_body)
      
      if (snap_data_success) {
        message(sprintf("   -> RTDB data updated: %s (date=%s, vol30=%s, garch=%s)", 
                        rtdb_encoded, 
                        as.character(latest_row$date),
                        ifelse(is.na(latest_row$vol30), "NA", sprintf("%.6f", latest_row$vol30)),
                        ifelse(is.na(latest_row$garch), "NA", sprintf("%.6f", latest_row$garch))))
      }
      
    } else {
      message(sprintf("   -> Skip marketSnapshot (FX/Crypto/Commodity handled by Delphi): %s", symbol))
    }
    
    # Metadata di RTDB - key harus encoded, tapi r2_key tetap format asli
    meta_update <- list(
      symbol = symbol,  # Simpan symbol asli untuk referensi
      first = r2_data$first_date,
      last = r2_data$last_date,
      count = r2_data$count,
      r2_key = r2_key,  # Format asli: historical/prices/BBCA.JK.json
      r2_url = sprintf("%s/%s", R2_PUBLIC_URL, r2_key)
    )
    
    # Path RTDB harus encoded
    meta_success <- patch_rtdb(paste0("metadata/dateRanges/", rtdb_encoded), meta_update)
    
    if (meta_success) {
      message(sprintf("   -> RTDB dateRanges updated: %s to %s (%d records)", 
                      meta_update$first, meta_update$last, meta_update$count))
    }
    
    message("   -> SUCCESS!")
  } else {
    message("   -> [ERROR] R2 upload failed!")
  }
  
  return(list(success = upload_success, new_records = nrow(new_data)))
}

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

main <- function() {
  cat("\n")
  cat("==============================================================================\n")
  cat("   INCREMENTAL STOCK DATA UPDATE: Yahoo Finance -> Cloudflare R2\n")
  cat("   Using Wrangler CLI for uploads\n")
  cat("   R2 filename: Minimal encoding (^JKSE -> _C_JKSE.json, BBCA.JK -> BBCA.JK.json)\n")
  cat("   RTDB key: Full encoding (BBCA.JK -> BBCA_JK, ^JKSE -> _C_JKSE)\n")
  cat(sprintf("   Local cache: %s\n", ifelse(USE_LOCAL_CACHE, "ENABLED", "DISABLED")))
  if (USE_LOCAL_CACHE) {
    cat(sprintf("   Cache dir:   %s\n", LOCAL_CACHE_DIR))
  }
  cat("==============================================================================\n\n")
  
  start_time <- Sys.time()
  
  # Step 0: Check Wrangler & Test R2 Connection
  message("[Step 0] Checking Wrangler CLI...")
  check_wrangler()
  
  message("\n   Testing R2 connection...")
  r2_ok <- test_r2_connection()
  
  if (!r2_ok) {
    stop("Failed to connect to R2 or bucket not found. Periksa wrangler login & nama bucket.")
  }
  message("   R2 connection successful!\n")
  
  # Ensure local cache directory exists (if enabled)
  if (USE_LOCAL_CACHE) {
    message("[Step 0b] Initializing local cache...")
    ensure_cache_dir()
    message(sprintf("   Local cache directory: %s\n", LOCAL_CACHE_DIR))
  } else {
    message("[Step 0b] Local cache DISABLED - will read directly from R2\n")
  }
  
  # Step 1: Read symbols from RTDB
  message("[Step 1] Reading symbols from Firebase RTDB...")
  sym_raw <- read_rtdb("metadata/symbols")
  #sym_raw <- as.list(fromJSON("/Users/bennyky/Documents/R/symbols_to_save.json"))
  
  if (is.null(sym_raw)) {
    stop("Failed to read symbols from RTDB.")
  }
  
  # Decode semua symbols ke format asli (dari RTDB yang encoded)
  all_syms <- unique(sapply(unlist(sym_raw), decode_ticker_from_rtdb))
  
  message(sprintf("   Found %d symbols to process", length(all_syms)))
  
  # Step 2: Read existing dateRanges
  message("\n[Step 2] Reading existing dateRanges from RTDB...")
  date_ranges <- read_rtdb("metadata/dateRanges")
  
  if (is.null(date_ranges)) {
    date_ranges <- list()
  } else {
    message(sprintf("   Found dateRanges for %d symbols", length(date_ranges)))
  }
  
  # Step 3: Process each symbol
  message("\n[Step 3] Processing symbols...")
  message("==============================================================================")
  
  total_symbols <- length(all_syms)
  success_count <- 0
  skip_count <- 0
  error_count <- 0
  total_new_records <- 0
  
#  for (i in seq_along(all_syms)) {
  for (i in 1:2) {
    sym <- all_syms[i]
    # RTDB key masih encoded untuk lookup date_ranges
    enc <- encode_ticker_for_rtdb(sym)
    
    result <- tryCatch({
      process_symbol_incremental(sym, date_ranges[[enc]], i, total_symbols, start_time)
    }, error = function(e) {
      message(sprintf("   -> [ERROR] Unexpected: %s", e$message))
      list(success = FALSE, new_records = 0)
    })
    
    if (result$success) {
      if (result$new_records > 0) {
        success_count <- success_count + 1
        total_new_records <- total_new_records + result$new_records
      } else {
        skip_count <- skip_count + 1
      }
    } else {
      error_count <- error_count + 1
    }
    
    Sys.sleep(0.3)
  }
  
  # Step 4: Update global metadata
  message("\n[Step 4] Updating global metadata...")
  
  # Update metadata
  patch_rtdb("metadata", list(
    lastUpdated = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    dataSource = "r2",
    r2Bucket = R2_BUCKET,
    r2HistPrefix = R2_HIST_PREFIX,
    r2PublicUrl = R2_PUBLIC_URL,
    r2FileFormat = "minimal_encoding"  # Hanya encode karakter URL-unsafe, titik dipertahankan
  ))
  
  # Update R2At timestamp di marketSnapshots/current
  r2at_success <- patch_rtdb("marketSnapshots/current", list(
    R2At = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  ))
  
  if (r2at_success) {
    message("   -> marketSnapshots/current/R2At timestamp updated")
  }
  
  # Cleanup temp directory
  if (dir.exists(TEMP_DIR)) {
    unlink(TEMP_DIR, recursive = TRUE)
  }
  
  # Final Summary
  total_elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
  
  # Count cache files (if cache enabled)
  cache_files <- 0
  if (USE_LOCAL_CACHE && dir.exists(LOCAL_CACHE_DIR)) {
    cache_files <- length(list.files(LOCAL_CACHE_DIR, pattern = "\\.json$"))
  }
  
  cat("\n")
  cat("==============================================================================\n")
  cat("                           EXECUTION COMPLETE\n")
  cat("==============================================================================\n")
  cat(sprintf("   Total symbols processed:   %d\n", total_symbols))
  cat(sprintf("   Successfully updated:      %d\n", success_count))
  cat(sprintf("   Skipped (up-to-date):      %d\n", skip_count))
  cat(sprintf("   Errors:                    %d\n", error_count))
  cat(sprintf("   Total new records added:   %d\n", total_new_records))
  cat(sprintf("   Total execution time:      %.1f minutes\n", total_elapsed))
  cat("------------------------------------------------------------------------------\n")
  cat(sprintf("   Local cache:               %s\n", ifelse(USE_LOCAL_CACHE, "ENABLED", "DISABLED")))
  if (USE_LOCAL_CACHE) {
    cat(sprintf("   Local cache files:         %d\n", cache_files))
    cat(sprintf("   Local cache location:      %s\n", LOCAL_CACHE_DIR))
  }
  cat("==============================================================================\n")
  cat(sprintf("   Data available at: %s/%s/prices/\n", R2_PUBLIC_URL, R2_HIST_PREFIX))
  cat(sprintf("   Examples:\n"))
  cat(sprintf("     - BBCA.JK  -> %s/%s/prices/BBCA.JK.json\n", R2_PUBLIC_URL, R2_HIST_PREFIX))
  cat(sprintf("     - ^JKSE    -> %s/%s/prices/_C_JKSE.json\n", R2_PUBLIC_URL, R2_HIST_PREFIX))
  cat("==============================================================================\n")
}

# ==============================================================================
# RUN
# ==============================================================================
main()
