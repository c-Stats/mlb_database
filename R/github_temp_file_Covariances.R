#' Compute covariance factors
#'
#' This function computes E[I_1 * I_2] / (E[I_1] E[I_2]), which is to be used to estimate the covariances when computing portfolio weights
#'
#'
#' @param path the path used by the webscrapper to save data
#' @return NULL
#' @export
#' @import data.table 
#' @import magrittr 
#' @import dplyr
#' @import RcppAlgos

compute_covariance_matrices <- function(path){

	path_check <- paste(path, "/MLB_Modeling/Scores/Clean_Data/Play_by_play_Database.rds", sep = "")
	if(!file.exists(path_check)){

		print(paste("ERROR: file missing at:", path_check), quote = FALSE)
		return(NULL)

	}
	play_by_play <- readRDS(paste(path, "/MLB_Modeling/Scores/Clean_Data/Play_by_play_Database.rds", sep = ""))


	#Estimates from 2020 and below since the model dates from 2021
	play_by_play <- play_by_play[which(as.integer(names(play_by_play)) <= 2020)]

	#Only do 5th and last inning as these are the innings used for pinnacle bets
	for(i in 1:length(play_by_play)){

			play_by_play[[i]][Inn. > 9, Inn. := 9]
			play_by_play[[i]][, Is_Last := lapply(.SD, function(x){

				out <- rep(FALSE, length(x))
				out[length(x)] <- TRUE
				return(out)


				}), by = c("ID", "Inn."), .SDcols = "Score_Home"]

			play_by_play[[i]] <- play_by_play[[i]][Is_Last == TRUE, c("ID", "Inn.", "Score_Home", "Score_Away")]
			play_by_play[[i]] <- play_by_play[[i]][Inn. == 5 | Inn. == 9]

	}

	play_by_play <- data.table::copy(dplyr::bind_rows(play_by_play))

	play_by_play[, Sum := Score_Home + Score_Away]
	play_by_play[, Spread_Home := Score_Home - Score_Away]
	play_by_play[, Spread_Away := -Spread_Home]

	data.table::setorderv(play_by_play, c("ID", "Inn."))

	#Make sure all matches contain the 5th and 9th inning
	play_by_play[, Keep := lapply(.SD, function(x){length(x) == 2}), by = "ID", .SDcols = "Inn."]
	play_by_play <- play_by_play[Keep == TRUE]
	play_by_play[, Keep := NULL]

	cnames <- c("Home", "Away")

	#Build the frames needed to compute E[I_1 * I_2] / sqrt(E[I_1] E[I_2])
	Bernoulli_matrices <- list()
	for(i in 1:2){

		temp <- data.table::copy(play_by_play[, c(paste(c("Score", "Spread"), cnames[i], sep = "_"), "Sum"), with = FALSE])
		names(temp)[1:2] <- c("Score", "Spread")

		#WINNER
		varname <- paste("WINNER", cnames[i])
		temp[, (varname) := as.numeric(Spread > 0)]

		#Spreads
		s <- seq(from = -4.5, to = 4.5, by = 1)
		for(value in s){

			varname <- paste("SPREAD", -value, cnames[i])
			temp[, (varname) := as.numeric(Spread > value)]

		}

		if(i == 1){

			#Sums
			s <- seq(from = 1.5, to = 15.5, by = 1)
			suffix <- c("Above", "Below")
			for(value in s){

				varname <- paste("SUM", suffix, value)
				temp[, (varname[1]) := as.numeric(Sum > value)]
				temp[, (varname[2]) := 1 - get(varname[1])]

			}

		}


		#Points
		s <- seq(from = 1.5, to = 10.5, by = 1)		
		suffix <- c("Above", "Below")
		for(value in s){

			varname <- paste("POINTS", suffix, value, cnames[i])
			temp[, (varname[1]) := as.numeric(Score > value)]
			temp[, (varname[2]) := 1 - get(varname[1])]

		}

		Bernoulli_matrices[[i]] <- as.matrix(temp[, -c(1:3)])

	}

	Bernoulli_matrices <- cbind(Bernoulli_matrices[[1]], Bernoulli_matrices[[2]])

	#Split per inning
	index <- play_by_play[Inn. == 5, which = TRUE]

	cnames <- c(paste(5, colnames(Bernoulli_matrices)), paste(9, colnames(Bernoulli_matrices)))
	Bernoulli_matrices <- cbind(Bernoulli_matrices[index, ], Bernoulli_matrices[-index, ])
	colnames(Bernoulli_matrices) <- cnames

	Bernoulli_matrices <- Bernoulli_matrices[, order(cnames)]
	colnames(Bernoulli_matrices) <- sort(cnames)
	cnames <- sort(cnames)

	#E[I_1 * I_2] / E[I_1] E[I_2]

	probability_ratios <- t(Bernoulli_matrices) %*% Bernoulli_matrices / nrow(Bernoulli_matrices)
	mu <- apply(Bernoulli_matrices, 2, mean)
	probability_ratios <- probability_ratios / mu %*% t(mu)
	rownames(probability_ratios) <- cnames

	path_save <- paste(path, "/MLB_Modeling/Scores/Clean_Data/Bernoulli_CovMat.rds", sep = "")
	saveRDS(probability_ratios, path_save)

	path_save <- paste(path, "/MLB_Modeling/Scores/Clean_Data/Bernoulli_CovMat2.rds", sep = "")
	saveRDS(var(Bernoulli_matrices), path_save)	


}


