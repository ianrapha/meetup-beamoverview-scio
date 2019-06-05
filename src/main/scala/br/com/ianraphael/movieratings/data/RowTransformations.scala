package br.com.ianraphael.movieratings.data

import br.com.ianraphael.movieratings.domain.database.{Movie, MovieRating, Rating}
import com.google.api.services.bigquery.model.TableRow

class RowTransformations() extends Serializable {
	def ratingLine(row: String): (Int, Rating) = {
		val values = row.split(",")
		val movieId = values(1).toInt

		(movieId, Rating(
			movieId = movieId,
			score = values(2).toDouble
		))
	}

	def sumRatings(a: Rating, b: Rating): Rating = {
		val sum = a.count + b.count

		a.copy(count = sum)
	}

	def movieRatingsToJSON(mr: MovieRating): String = {
		mr.toJSON
	}

	def movieRatingsToTableRow(mr: MovieRating): TableRow = {
		val row = new TableRow()
		row.put("movie_id", mr.movie_id)
		row.put("movie_name", mr.movie_name)
		row.put("rating", mr.rating)
		row.put("rating_count", mr.rating_count)

		row
	}

	def movieLine(row: String): (Int, Movie) = {
		val values = row.replaceAll(", ", " ").split(",")
		val id = values(0).toInt

		(id, Movie(
			id = id,
			name = values(1)
		))
	}

	def movieScoreKeyRatingValue(r: (Int, Rating)): ((Int, String), Rating) = {
		((r._1, r._2.score.toString), r._2)
	}

	def movieKeyRatingValue(r: ((Int, String), Rating)): (Int, Rating) = {
		(r._1._1, r._2)
	}

	def movieRating(row: (Int, (Movie, Rating))): MovieRating = {
		val m = row._2._1
		val r = row._2._2

		MovieRating(
			movie_id = m.id,
			movie_name = m.name,
			rating = r.score,
			rating_count = r.count
		)
	}
}
