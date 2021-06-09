#' Simulates an inning
#'
#' This function simmulates a single inning
#'
#' Note: you need to initiate "markov" with Extract_Markov_Chain_Data(...)
#'
#'
#' @param inn The inning
#' @param team home or away
#' @return A simmulated inning
#' @export
#' @import data.table 
#' @import magrittr 
#' @import dplyr
#'


simmulate_inning <- function(inn, team){

	states <- c(1)
	is_extra <- c(FALSE)

	game_over <- FALSE
	n <- 0

	while(!game_over){

		#Move against pitcher
		p_next <- markov[[team]][[inn]]$first[[n %% length(markov[[team]][[inn]]$first) + 1]][states[length(states)], ]
		p_next <- cumsum(p_next)

		next_state <- min(which(p_next >= runif(1)))

		states <- c(states, as.numeric(next_state))
		is_extra <- c(is_extra, FALSE)

		if(next_state == nrow(play_by_play$states)){

			n <- n + 1
			game_over <- TRUE
			next

		}

		#Extra move (base steal, ect.)
		#Not applicable for first event
		if(n > 0){

			p_next <- markov[[team]][[inn]]$extra[[n %% length(markov[[team]][[inn]]$first) + 1]][states[length(states)], ]
			p_next <- cumsum(p_next)		

			next_state <- which(p_next > runif(1))[1]

			if(next_state != states[length(states)]){

				states <- c(states, as.numeric(next_state))
				is_extra <- c(is_extra, TRUE)

			}

			n <- n + 1

		}


		if(next_state == nrow(play_by_play$states)){

			game_over <- TRUE

		}


	}

	inning <- data.table::as.data.table(cbind(states, !is_extra))
	names(inning)[2] <- "Batter_Entering_Play"
	inning[c(1, nrow(inning)), Batter_Entering_Play := 0]
	
	index <- match(inning$states, play_by_play$states$i)
	inning[, Base := play_by_play$states$Base[index]]

	inning[, Out := diff(c(0,play_by_play$states$Outs[index]))]
	inning[, On_Base := play_by_play$states$On_Base[index]]

	scored <- -diff(inning$On_Base[-nrow(inning)]) + inning$Batter_Entering_Play[-c(1, nrow(inning))] - inning$Out[-c(1, nrow(inning))]

	inning[c(1, nrow(inning)), Scored := 0]
	inning[-c(1, nrow(inning)), Scored := scored]
	inning[, Scored := sapply(Scored, function(x){max(x, 0)})]

	inning[, Points := cumsum(Scored)]
	inning[nrow(inning), Points := inning$Points[nrow(inning) - 1]]
	inning[nrow(inning), Scored := 0]

	inning[, Vs_Pitch := as.logical(Batter_Entering_Play)]

	return(inning[, c("states", "Vs_Pitch", "Base", "Out", "Scored", "Points")])


}


#' Simulates an entire game
#'
#' This function simmulates an entire game
#'
#' Note: you need to initiate "markov" with Extract_Markov_Chain_Data(...)
#'
#'
#' @return A simmulated game
#' @export
#' @import data.table 
#' @import magrittr 
#' @import dplyr
#'


simmulate_game <- function(){

	game_over <- FALSE

	teams <- c("home", "away")

	inn <- 1
	game <- list(home = simmulate_inning(inn, "home"),
					away = simmulate_inning(inn, "away"))

	for(i in 1:2){

		game[[i]][, Inn. := inn]

	}

	while(!game_over){

		inn <- inn + 1
		for(i in 1:2){

			sim <- simmulate_inning(min(inn, 9), teams[i])
			sim[, Inn. := inn]

			game[[i]] <- rbind(game[[i]], sim)
			game[[i]][, Points := cumsum(Scored)]

		}


		if(inn >= 9 & game$home$Points[nrow(game$home)] != game$away$Points[nrow(game$away)]){

			game_over <- TRUE

		}


	}

	return(game)

}