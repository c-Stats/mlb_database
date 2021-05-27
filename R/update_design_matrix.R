#' Update the regression data
#'
#' This function updates the design matrix, the score and the bookmakers data needed for regression purposes
#'
#'
#' @param scrapper_path the path used by the webscrapper to save data
#' @param avg_number a tag used to save file (i.e.: to distinguish between different time windows used to compute averages)
#' @return NULL
#' @export
#' @import data.table 
#' @import magrittr 
#' @import dplyr
#'
update_design_matrix <- function(scrapper_path, avg_number, params = list(n_season = 2,
																			n_match_bat = 120,
																			n_match_pitch = 120)){

	n <- avg_number

	################################################
	########### Load database       ################
	################################################

	load_path <- paste(scrapper_path, "/MLB_Modeling/Scores/Clean_Data/Seasonal_Database.rds", sep = "")
	DB <- readRDS(load_path)

	load_path <- paste(scrapper_path, "/MLB_Modeling/Scores/Clean_Data/DB_Scores.rds", sep = "")
	scores <- readRDS(load_path)

	################################################
	################################################


	################################################
	########### REGRESSION FRAME    ################
	################################################

	#Check if frame needs to be created, or updated
	directory <- paste(scrapper_path, "/MLB_Modeling/Regression/", n, sep = "")
	if(!dir.exists(directory)){

		dir.create(directory)

	}

	path_save <- paste(directory, "/R_regression_matrix.rds", sep = "")
	if(file.exists(path_save)){

		update <- TRUE

		existing_data <- readRDS(path_save)
		ID_done <- existing_data$Y$ID
		data.table::setkey(scores, "ID")
		scores <- scores[-scores[ID_done, which = TRUE]]
		scores <- scores[Date >= max(existing_data$Y$Date)]

		print("Existing design matrix found; updating database...", quote = FALSE)

	} else {

		print("No existing design matrix found; initiating new database...", quote = FALSE)
		update <- FALSE

	}

	#Weed-out unusable data
	scores[, Missing_Data := FALSE]

	if(nrow(scores) == 0){

		print("Database is up to date.", quote = FALSE)
		return(NULL)

	}

	sample_row <- mlbDatabase::query_stats(date = NULL, lineup = NULL, params = params, id = as.numeric(scores$ID[1]))
	ncols <- ncol(sample_row)
	nrows <- nrow(scores)

	X <- matrix(0.0, nrows, ncols)
	colnames(X) <- colnames(sample_row)
	X <- data.table::as.data.table(X)
	sd_col <- c(1:ncol(X))

	print(paste(nrow(X), "matches are to be processed."), quote = FALSE)

	data.table::setkey(scores, "ID")
	pb <- txtProgressBar(min = 0, max = nrow(X), style = 3)
	n_miss <- 0
	for(i in 1:nrow(X)){

		row_val <- try(mlbDatabase::query_stats(date = NULL, lineup = NULL, params = params, id = scores$ID[i]), silent = TRUE)
		if(class(row_val)[1] == "try-error"){

			print("Lineup not found.", quote = FALSE)
			data.table::set(X, i = as.integer(i), j = sd_col, value = NA)

		} else {

			if(nrow(row_val) == 0){

				data.table::set(X, i = as.integer(i), j = sd_col, value = NA)

			} else {

				data.table::set(X, i = as.integer(i), j = sd_col, value = row_val[1])

			}

			if(any(is.na(X[i]))){

				n_miss <- n_miss + 1
				p_miss <- round(100 * n_miss / i, 2)
				print(paste("Warning: NA values (", p_miss, "%) ...", sep = ""), quote = FALSE)
				scores[i, "Missing_Data"] <- TRUE

			}

		}


		setTxtProgressBar(pb, i)

	}


	rmv <- unique(which(is.na(X), arr.ind = TRUE)[, 1])


	keep <- which(!scores$Missing_Data)
	scores <- scores[keep]
	X <- X[keep]

	rmv <- unique(which(is.na(X), arr.ind = TRUE)[, 1])
	if(length(rmv) > 0){

		X <- X[-rmv]
		scores <- scores[-rmv]

	}

	index <- order(scores$Date)
	scores <- scores[index]
	X <- X[index]

	if(update){

		scores <- rbind(existing_data$Y[, names(scores), with = FALSE],
							scores)

		X <- rbind(existing_data$X[, names(X), with = FALSE],
							X)

	}

	################################################
	################################################



	################################################
	########### SAVE                ################
	################################################

	directory <- paste(scrapper_path, "/MLB_Modeling/Regression/", n, sep = "")
	if(!dir.exists(directory)){

		dir.create(directory)

	}

	path_save <- paste(directory, "/R_regression_matrix.rds", sep = "")
	saveRDS(list(X = X, 
					Y = scores),
					path_save)


	################################################
	################################################

}

