#' Extract aggregated team statistics (Home, Visiting)
#'
#' This function returns a row containing the aggregated player statistics of
#' the home and visiting teams' batters and pitchers. It is used to build the design matrix.
#'
#' Note: you need to initiate DB (.../MLB_Modeling/Scores/Clean_Data/Seasonal_Database.rds) and 
#' scores (/MLB_Modeling/Scores/Clean_Data/DB_Scores.rds) prior to running this function.
#'
#'
#' @param date the date at which the match takes (or took) place
#' @param lineup a list of size 3 containing: home/away = list(bat = ..., pitch = ...), teams = list(home = ..., away =...). The most inner elements are vectors containing characters, i.e.: names.
#' @param params a list of size 3 containing: n_season = ..., n_match_bat = ..., n_match_pitch = ... . These are the time-windows used to compute averages.
#' @param id the match ID. Defaults to NULL; if specified, the lineup will be retrieved from historical files. (I.e.: use IDs to build a design matrix X post-webscrapping.)
#' @return A single row of the design matrix X given two teams and two rosters.
#' @export
#' @import data.table 
#' @import magrittr 
#' @import dplyr
#'
query_stats <- function(date, lineup, params, id = NULL){

	`%>%` <- magrittr::`%>%`
	`:=` <- data.table::`:=`

	#Construct the lineup if an ID is provided instead of a lineup
	if(!is.null(id)){

		id <- as.integer(id)

		keys <- data.table::key(scores)
		if(is.null(keys) | length(keys) > 1){

			data.table::setkey(scores, "ID")

		} else if(keys != "ID"){

			data.table::setkey(scores, "ID")

		}	

		game_info <- scores[.(id)]	
		teams <- unlist(game_info[, c("Team_Home", "Team_Away")])
		date <- game_info$Date
		season <- as.character(game_info$Season)

		for(team in teams){

			for(j in 1:2){

				keys <- data.table::key(DB[[season]][[team]][[j]])
				if(is.null(keys) | length(keys) > 1){

					data.table::setkey(DB[[season]][[team]][[j]], "ID")

				} else if(keys != "ID"){

					data.table::setkey(DB[[season]][[team]][[j]], "ID")

				}

			}

		}

		lineup <- list()
		for(a in 1:2){

			lineup[[a]] <- list()

			for(b in 1:2){

				lineup[[a]][[b]] <- DB[[season]][[teams[a]]][[b]][.(id)][Announced == "Yes"]$Name

			}
			names(lineup[[a]]) <- c("bat", "pitch")

		}
		lineup[[3]] <- list(home = as.character(teams[1]), away = as.character(teams[2]))
		names(lineup) <- c("home", "away", "teams")

	}

	season <- stringr::str_split(date, "-", simplify = TRUE)[1]

	seasons <- (as.integer(season) - params$n_season + 1):as.integer(season)
	seasons <- as.character(seasons)
	seasons <- seasons[which(seasons %in% names(DB))]

	frames <- list()
	for(i in 1:2){

		#Check if DB tables are keyed
		for(s in seasons){

			for(j in 1:4){

				keys <- data.table::key(DB[[s]][[lineup$teams[[i]]]][[j]])
				if(is.null(keys) | length(keys) > 1){

					data.table::setkey(DB[[s]][[lineup$teams[[i]]]][[j]], "Name")

				} else if(keys != "Name"){

					data.table::setkey(DB[[s]][[lineup$teams[[i]]]][[j]], "Name")

				}

			}

		}


		#Get the match number
		match_n <- DB[[season]][[lineup$teams[[i]]]]$Pitch[Date < date]
		match_n <- match_n[which.max(Nth_match)]$Nth_match[1] + 1
		
		frames[[i]] <- list()
		n_match_total <- c(0)

		#Query date range
		#Announced players 
		dummy <- list()
		for(s in seasons){

			dummy[[length(dummy) + 1]] <- DB[[s]][[lineup$teams[[i]]]]$Bat[.(lineup[[i]]$bat)]
			dummy[[length(dummy)]] <- dummy[[length(dummy)]][complete.cases(dummy[[length(dummy)]]), ]
			dummy[[length(dummy)]][, Nth_match_total := Nth_match + sum(n_match_total)]

			if(s == season){match_n <- match_n + sum(n_match_total)}
			n_match_total <- c(n_match_total, max(dummy[[length(dummy)]]$Nth_match))
			
		}

		frames[[i]]$Bat_A <- dplyr::bind_rows(dummy)
		frames[[i]]$Bat_A <- frames[[i]]$Bat_A[Nth_match_total %between% c(match_n - params$n_match_bat, match_n - 1)]

		#Query date range
		#Unannounced players 
		dummy <- list()
		k <- 1
		for(s in seasons){

			dummy[[length(dummy) + 1]] <- DB[[s]][[lineup$teams[[i]]]]$Bat_Relief[Date < date]
			dummy[[length(dummy)]] <- dummy[[length(dummy)]][complete.cases(dummy[[length(dummy)]]), ]
			dummy[[length(dummy)]][, Nth_match_total := Nth_match + n_match_total[k]]

			k <- k + 1
			
		}

		frames[[i]]$Bat_UA <- dplyr::bind_rows(dummy)
		data.table::setkey(frames[[i]]$Bat_UA, "Name")
		frames[[i]]$Bat_UA <- frames[[i]]$Bat_UA[!.(lineup[[i]]$bat)]
		frames[[i]]$Bat_UA <- frames[[i]]$Bat_UA[Nth_match_total %between% c(match_n - params$n_match_bat, match_n - 1)]


		#Get the most recent Geom_MLE variable for unanounced players
		data.table::setkey(frames[[i]]$Bat_UA, NULL)
		data.table::setorder(frames[[i]]$Bat_UA, Date)
		cols <- match(c("Geom_MLE", "Nth_match_total"), colnames(frames[[i]]$Bat_UA))
		n_match_not_played <- frames[[i]]$Bat_UA[, lapply(.SD, last), .SDcols = cols, by = Name]
		n_match_not_played[, Mnp := match_n - Nth_match_total]
		n_match_not_played[Geom_MLE == 1, Geom_MLE := 0.5]
		n_match_not_played[Geom_MLE == 0, Geom_MLE := 10^(-6)]

		n_match_not_played[, P_val := 1 - pgeom(Mnp, Geom_MLE)]
		n_match_not_played <- n_match_not_played[P_val >= 0.05 & Mnp <= 20]


		data.table::setkeyv(n_match_not_played, c("Name"))
		data.table::setkeyv(frames[[i]]$Bat_UA, c("Name"))

		frames[[i]]$Bat_UA <- frames[[i]]$Bat_UA[.(unique(n_match_not_played$Name))]
		frames[[i]]$Bat_UA[n_match_not_played, Geom_MLE := i.Geom_MLE]

		original_weight_sum <- sum(frames[[i]]$Bat_UA$Weight)
		frames[[i]]$Bat_UA[, Weight := Weight * Geom_MLE]
		correcting_factor <- original_weight_sum / sum(frames[[i]]$Bat_UA$Weight)
		frames[[i]]$Bat_UA[, Weight := Weight * (correcting_factor)]


		#Compute the average, by player
		#Then the weighted average
		index_var <- which(sapply(frames[[i]]$Bat_A, is.numeric))
		index_var <- index_var[which(names(index_var) != "ID" & names(index_var) != "Season")]
		var_names <- names(index_var)

		#For starting players, compute the weights given they were announced
		starting_weights <- frames[[i]]$Bat_A[Announced == "Yes", c("Name","Weight"), with = FALSE][, lapply(.SD, weighted.mean, w = Weight), .SDcols = c("Weight"), by = Name]
		data.table::setkey(starting_weights, "Name")	


		frames[[i]]$Bat_A[, (var_names) := lapply(.SD, as.numeric), .SDcols = index_var]
		frames[[i]]$Bat_A <- frames[[i]]$Bat_A[, lapply(.SD, mean), .SDcols = index_var, by = Name]
		data.table::setkey(frames[[i]]$Bat_A, "Name")
		frames[[i]]$Bat_A[starting_weights, Weight := i.Weight]

		frames[[i]]$Bat_A[, Name := NULL]
		frames[[i]]$Bat_A <- frames[[i]]$Bat_A[, lapply(.SD, weighted.mean, w = Weight), .SDcols = c(1:ncol(frames[[i]]$Bat_A))]

		frames[[i]]$Bat_UA[, (var_names) := lapply(.SD, as.numeric), .SDcols = index_var]
		frames[[i]]$Bat_UA <- frames[[i]]$Bat_UA[, lapply(.SD, mean), .SDcols = index_var, by = Name]
		frames[[i]]$Bat_UA[, Name := NULL]
		frames[[i]]$Bat_UA <- frames[[i]]$Bat_UA[, lapply(.SD, weighted.mean, w = Weight), .SDcols = c(1:ncol(frames[[i]]$Bat_A))]

		#Combine the A and UA stats
		to_del <- c("Nth_match", "Geom_MLE", "Nth_match_total")
		frames[[i]]$Bat <- dplyr::bind_rows(frames[[i]])[, lapply(.SD, weighted.mean, w = Weight), .SDcols = var_names] %>%
							.[, (to_del) := NULL]
		frames[[i]]$Bat_UA <- NULL
		frames[[i]]$Bat_A <- NULL					

		suffix <- paste("_BAT_", toupper(names(lineup$teams)[i]), sep = "")
		colnames(frames[[i]]$Bat) <- paste(colnames(frames[[i]]$Bat), suffix, sep = "")




		#Query date range
		#Announced players 
		dummy <- list()
		k <- 1
		for(s in seasons){

			dummy[[length(dummy) + 1]] <- DB[[s]][[lineup$teams[[i]]]]$Pitch[.(lineup[[i]]$pitch)]
			dummy[[length(dummy)]] <- dummy[[length(dummy)]][complete.cases(dummy[[length(dummy)]]), ]
			dummy[[length(dummy)]][, Nth_match_total := Nth_match + n_match_total[k]]

			k <- k + 1

		}

		frames[[i]]$Pitch_A <- dplyr::bind_rows(dummy)
		frames[[i]]$Pitch_A <- frames[[i]]$Pitch_A[Nth_match_total %between% c(match_n - params$n_match_pitch, match_n - 1)]

		#Query date range
		#Unannounced players 
		dummy <- list()
		k <- 1
		for(s in seasons){

			dummy[[length(dummy) + 1]] <- DB[[s]][[lineup$teams[[i]]]]$Pitch_Relief[Date < date]
			dummy[[length(dummy)]] <- dummy[[length(dummy)]][complete.cases(dummy[[length(dummy)]]), ]
			dummy[[length(dummy)]][, Nth_match_total := Nth_match + n_match_total[k]]

			k <- k + 1
			
		}

		frames[[i]]$Pitch_UA <- dplyr::bind_rows(dummy)
		data.table::setkey(frames[[i]]$Pitch_UA, "Name")
		frames[[i]]$Pitch_UA <- frames[[i]]$Pitch_UA[!.(lineup[[i]]$pitch)]
		frames[[i]]$Pitch_UA <- frames[[i]]$Pitch_UA[Nth_match_total %between% c(match_n - params$n_match_pitch, match_n - 1)]


		#Get the most recent Geom_MLE variable for unanounced players
		data.table::setkey(frames[[i]]$Pitch_UA, NULL)
		data.table::setorder(frames[[i]]$Pitch_UA, Date)
		cols <- match(c("Geom_MLE", "Nth_match_total"), colnames(frames[[i]]$Pitch_UA))
		n_match_not_played <- frames[[i]]$Pitch_UA[, lapply(.SD, last), .SDcols = cols, by = Name]
		n_match_not_played[, Mnp := match_n - Nth_match_total]
		n_match_not_played[Geom_MLE == 1, Geom_MLE := 0.5]
		n_match_not_played[Geom_MLE == 0, Geom_MLE := 10^(-6)]

		n_match_not_played[, P_val := 1 - pgeom(Mnp, Geom_MLE)]
		n_match_not_played <- n_match_not_played[P_val >= 0.05 & Mnp <= 20]


		data.table::setkeyv(n_match_not_played, c("Name"))
		data.table::setkeyv(frames[[i]]$Pitch_UA, c("Name"))

		frames[[i]]$Pitch_UA <- frames[[i]]$Pitch_UA[.(unique(n_match_not_played$Name))]
		frames[[i]]$Pitch_UA[n_match_not_played, Geom_MLE := i.Geom_MLE]

		original_weight_sum <- sum(frames[[i]]$Pitch_UA$Weight)
		frames[[i]]$Pitch_UA[, Weight := Weight * Geom_MLE]
		correcting_factor <- original_weight_sum / sum(frames[[i]]$Pitch_UA$Weight)
		frames[[i]]$Pitch_UA[, Weight := Weight * (correcting_factor)]


		#Compute the average, by player
		#Then the weighted average
		index_var <- which(sapply(frames[[i]]$Pitch_A, is.numeric))
		index_var <- index_var[which(names(index_var) != "ID" & names(index_var) != "Season")]
		var_names <- names(index_var)

		#For starting players, compute the weights given they were announced
		starting_weights <- frames[[i]]$Pitch_A[Announced == "Yes", c("Name","Weight"), with = FALSE][, lapply(.SD, weighted.mean, w = Weight), .SDcols = c("Weight"), by = Name]
		data.table::setkey(starting_weights, "Name")	


		frames[[i]]$Pitch_A[, (var_names) := lapply(.SD, as.numeric), .SDcols = index_var]
		frames[[i]]$Pitch_A <- frames[[i]]$Pitch_A[, lapply(.SD, mean), .SDcols = index_var, by = Name]
		data.table::setkey(frames[[i]]$Pitch_A, "Name")
		frames[[i]]$Pitch_A[starting_weights, Weight := i.Weight]

		frames[[i]]$Pitch_A[, Name := NULL]

		frames[[i]]$Pitch_UA[, (var_names) := lapply(.SD, as.numeric), .SDcols = index_var]
		frames[[i]]$Pitch_UA <- frames[[i]]$Pitch_UA[, lapply(.SD, mean), .SDcols = index_var, by = Name]
		frames[[i]]$Pitch_UA[, Name := NULL]
		frames[[i]]$Pitch_UA <- frames[[i]]$Pitch_UA[, lapply(.SD, weighted.mean, w = Weight), .SDcols = c(1:ncol(frames[[i]]$Pitch_A))]

		#Combine the A and UA stats
		to_del <- c("Nth_match", "Geom_MLE", "Nth_match_total")
		frames[[i]]$Pitch <- dplyr::bind_rows(frames[[i]][-1])[, lapply(.SD, weighted.mean, w = Weight), .SDcols = var_names] %>%
							.[, (to_del) := NULL]
		frames[[i]]$Pitch_UA <- NULL
		frames[[i]]$Pitch_A <- NULL					

		suffix <- paste("_PITCH_", toupper(names(lineup$teams)[i]), sep = "")
		colnames(frames[[i]]$Pitch) <- paste(colnames(frames[[i]]$Pitch), suffix, sep = "")


		#Get the team info
		frames[[i]]$Team_Stats <- DB[[season]][[lineup$teams[[i]]]]$Team_Stats[Date < date, c("Date", "Win_p", "PPG", "PPG_vs", "PPG_diff", "Streak")]
		frames[[i]]$Team_Stats <- frames[[i]]$Team_Stats[which.max(Date)]
		frames[[i]]$Team_Stats[, Date := NULL]

		colnames(frames[[i]]$Team_Stats) <- paste(colnames(frames[[i]]$Team_Stats), "TEAM_STATS", toupper(names(lineup$teams)[i]), sep = "_")

		frames[[i]] <- bind_cols(frames[[i]])

	}

	output <- bind_cols(frames)
	output[, (colnames(output)) := lapply(.SD, as.numeric), .SDcols = c(1:ncol(output))]

	return(output)

}

