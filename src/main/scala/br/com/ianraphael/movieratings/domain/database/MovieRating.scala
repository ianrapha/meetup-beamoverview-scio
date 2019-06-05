package br.com.ianraphael.movieratings.domain.database

import com.google.gson.Gson

case class MovieRating(
	movie_id: Int,
	movie_name: String,
	rating: Double,
	rating_count: Long
) {
	def toJSON: String = {
		new Gson().toJson(this)
	}
}