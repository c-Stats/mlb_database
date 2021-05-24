#' Initiate or update the play-by-play database
#'
#' This function creates the database with the play-by-play data
#'
#'
#' @param path the path used by the webscrapper to save data
#' @return NULL
#' @export
#' @import data.table 
#' @import magrittr 
#' @import dplyr
#'



##############################################################################
##############################################################################
################								##############################
################  LOAD DATA                     ##############################
################								##############################
##############################################################################
##############################################################################

process_PlayByPlay_data <- function(path){

	#Saving directory
	update <- FALSE
	old_pbp <- NULL
	years_done <- c()

	saving_path <- paste(path, "/MLB_Modeling/Scores/Clean_Data/Play_by_play_Database.rds", sep = "")
	if(file.exists(saving_path)){

		print("Updating existing file.", quote = FALSE)

		update <- TRUE
		old_pbp <- readRDS(saving_path)
		years_done <- names(old_pbp)[-length(old_pbp)]

	} else {

		print("Building database from scratch...", quote = FALSE)

	}


	#Load data
	bat_lineup <- data.table::fread(paste(path, "/MLB_Modeling/Bat/Clean_Data/Lineups_BR.csv", sep = ""))
	pitch_lineup <- data.table::fread(paste(path, "/MLB_Modeling/Pitch/Clean_Data/Lineups_BR.csv", sep = ""))

	DB <- readRDS(paste(path, "/MLB_Modeling/Scores/Clean_Data/Seasonal_Database.rds", sep = ""))

	play_by_play_directory <- paste(path, "/MLB_Modeling/Scores/Play_by_play", sep = "")
	play_by_play_files <- list.files(path = play_by_play_directory, full.names = TRUE, recursive = TRUE)[-1]

	years <- sapply(play_by_play_files, function(x){
														s <- stringr::str_split(x, "/")[[1]]
														s <- s[length(s)]
														return(as.character(stringr::str_replace(s, ".csv", "")))

		})

	play_by_play <- list()
	i <- 1
	pbp_names <- c()
	for(file in play_by_play_files){

		if(years[i] %in% years_done){

			i <- i + 1
			next

		}

		play_by_play[[length(play_by_play) + 1]] <- data.table::fread(file)
		pbp_names <- c(pbp_names, years[i])

		i <- i + 1

	}

	names(play_by_play) <- pbp_names





	##############################################################################
	##############################################################################
	################								##############################
	################  DATA CLEANING                 ##############################
	################								##############################
	##############################################################################
	##############################################################################

	l3_char <- function(x){

		substr(x, nchar(x) - 2, nchar(x))

	}

	#Tag team at bat / team at pitch
	for(i in 1:length(play_by_play)){

		IDs <- unique(play_by_play[[i]]$ID)
		data.table::setkey(play_by_play[[i]], ID)

		#Replace final scores with live ones
		temp <- stringr::str_split(play_by_play[[i]]$Score, "-", simplify = TRUE)
		play_by_play[[i]][, Score_Home := as.integer(temp[, 1])]
		play_by_play[[i]][, Score_Away := as.integer(temp[, 2])]


		print(paste("Processing data from year ", names(play_by_play)[i], "...", sep = ""), quote = FALSE)

		pb <- txtProgressBar(min = 0, max = length(IDs), style = 3)
		count <- 0
		for(id in IDs){

			#Retrieve play-by-play frame
			index_pbp <- play_by_play[[i]][.(id), which = TRUE]
			teams <- play_by_play[[i]][index_pbp[1], c("Team_Home", "Team_Away"), with = FALSE]

			batters <- unique(play_by_play[[i]][index_pbp]$Player)
			batters_fname <- sapply(batters, function(x){stringr::str_split(x, " ")[[1]][2]})
			batters_i <- sapply(batters, function(x){substr(x, 1, 1)})

			pitchers <- unique(play_by_play[[i]][index_pbp]$Pitcher)
			pitchers_fname <- sapply(pitchers, function(x){stringr::str_split(x, " ")[[1]][2]})
			pitchers_i <- sapply(pitchers, function(x){substr(x, 1, 1)})

			#Retrieve batters
			k <- data.table::key(DB[[names(play_by_play)[i]]][[teams$Team_Home[1]]]$Bat)
			if(length(k) > 1){

				data.table::setkey(DB[[names(play_by_play)[i]]][[teams$Team_Home[1]]]$Bat, ID)

			} else if(k[1] != "ID"){

				data.table::setkey(DB[[names(play_by_play)[i]]][[teams$Team_Home[1]]]$Bat, ID)

			}

			k <- data.table::key(DB[[names(play_by_play)[i]]][[teams$Team_Away[1]]]$Bat)
			if(length(k) > 1){

				data.table::setkey(DB[[names(play_by_play)[i]]][[teams$Team_Away[1]]]$Bat, ID)

			} else if(k[1] != "ID"){

				data.table::setkey(DB[[names(play_by_play)[i]]][[teams$Team_Away[1]]]$Bat, ID)

			}

			batters_tags <- list(home = DB[[names(play_by_play)[i]]][[teams$Team_Home[1]]]$Bat[.(id)]$Name,
								away = DB[[names(play_by_play)[i]]][[teams$Team_Away[1]]]$Bat[.(id)]$Name)

			batters_tags <- unlist(batters_tags)

			if(length(batters_tags) == 2){next}

			batters_initials <- sapply(batters_tags, function(x){substr(x, 1, 1)})

			

			#Retrieve pitchers
			k <- data.table::key(DB[[names(play_by_play)[i]]][[teams$Team_Home[1]]]$Pitch)
			if(length(k) > 1){

				data.table::setkey(DB[[names(play_by_play)[i]]][[teams$Team_Home[1]]]$Pitch, ID)

			} else if(k[1] != "ID"){

				data.table::setkey(DB[[names(play_by_play)[i]]][[teams$Team_Home[1]]]$Pitch, ID)

			}

			k <- data.table::key(DB[[names(play_by_play)[i]]][[teams$Team_Away[1]]]$Pitch)
			if(length(k) > 1){

				data.table::setkey(DB[[names(play_by_play)[i]]][[teams$Team_Away[1]]]$Pitch, ID)

			} else if(k[1] != "ID"){

				data.table::setkey(DB[[names(play_by_play)[i]]][[teams$Team_Away[1]]]$Pitch, ID)

			}

			pitchers_tags <- list(home = DB[[names(play_by_play)[i]]][[teams$Team_Home[1]]]$Pitch[.(id)]$Name,
								away = DB[[names(play_by_play)[i]]][[teams$Team_Away[1]]]$Pitch[.(id)]$Name)
			
			pitchers_tags <- unlist(pitchers_tags)
			pitchers_initials <- sapply(pitchers_tags, function(x){substr(x, 1, 1)})


			#Perform match
			name_index <- list()
			name_index$Bat <- sapply(c(1:length(batters)), function(i){which(batters_initials == batters_i[i] & 
																				grepl(batters_fname[i], batters_tags, fixed = TRUE))})
			name_index$Pitch <- sapply(c(1:length(pitchers)), function(i){which(pitchers_initials == pitchers_i[i] & 
																				grepl(pitchers_fname[i], pitchers_tags, fixed = TRUE))})

			name_index <- lapply(name_index, function(x){if(is.list(x)){unlist(x)} else {x}})																		

			keep <- lapply(name_index, function(x){which(!is.na(x))})
			name_index <- lapply(c(1:2), function(i){name_index[[i]][keep[[i]]]})

			names <- list(Bat = batters,
							Pitch = pitchers)
			names <- lapply(c(1:2), function(i){names[[i]][keep[[i]]]})

			traduction <- list(Bat = batters_tags[name_index[[1]]],
								Pitch = pitchers_tags[name_index[[2]]])


			#Fill table
			for(j in 1:length(names[[1]])){

				index2 <- play_by_play[[i]][index_pbp][Player == names[[1]][j], which = TRUE]
				play_by_play[[i]][index_pbp[index2], Batter_Name := traduction[[1]][j]]

			}

			for(j in 1:length(names[[2]])){

				index2 <- play_by_play[[i]][index_pbp][Pitcher == names[[2]][j], which = TRUE]
				play_by_play[[i]][index_pbp[index2], Pitcher_Name := traduction[[2]][j]]

			}

			count <- count + 1
			setTxtProgressBar(pb, count)	

		}

		play_by_play[[i]][, Team_Pitch := sapply(Pitcher_Name, l3_char)]
		play_by_play[[i]][, Team_Bat := sapply(Batter_Name, l3_char)]

		play_by_play[[i]] <- play_by_play[[i]][!is.na(Batter_Name)]


	}


	if(!update){

		saveRDS(play_by_play, saving_path)

	} else {

		#Update the old file
		replace <- match(names(play_by_play), names(old_pbp))

		for(i in 1:length(replace)){

			#In case of name match
			if(!is.na(replace[i])){

				old_pbp[[replace[i]]] <- play_by_play[[i]]

			#In case of a new value (i.e.: adding a new season)
			} else {

				old_pbp[[length(old_pbp) + 1]] <- play_by_play[[i]]
				names(old_pbp)[length(old_pbp)] <- names(play_by_play)[i]

			}

		}

		saveRDS(old_pbp, saving_path)

	}

}