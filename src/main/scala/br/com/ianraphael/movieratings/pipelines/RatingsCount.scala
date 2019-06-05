package br.com.ianraphael.movieratings.pipelines

import br.com.ianraphael.movieratings.data.RowTransformations
import br.com.ianraphael.movieratings.handlers.BigQuery
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery._

object RatingsCount {
	def main(cmdlineArgs: Array[String]): Unit = {
		val (sc, args) = ContextAndArgs(cmdlineArgs)

		sc.setJobName("movieratings-count")

		val trf = new RowTransformations()

		val moviesRaw = sc.textFile(args("moviesFile")).withName("Movies FileRead")
		val movies = moviesRaw.map(trf.movieLine)

		val ratingsRaw = sc.textFile(args("ratingsFile"))
		val ratings = ratingsRaw.map(trf.ratingLine)
		val ratingsSum = ratings.map(trf.movieScoreKeyRatingValue).reduceByKey(trf.sumRatings).map(trf.movieKeyRatingValue)

		val movieRatings = movies.join(ratingsSum).map(trf.movieRating)
		val movieRatingsJSON = movieRatings.map(trf.movieRatingsToJSON)
		val movieRatingsTableRow = movieRatings.map(trf.movieRatingsToTableRow)

		movieRatingsJSON
			.withName("MovieRatingsToStorage")
			.saveAsTextFile(args("movieRatingsFile"), numShards = 1)

		movieRatingsTableRow
			.withName("MovieRatingsToBigQuery")
			.saveAsBigQuery(BigQuery.getMovieRatingsTable, BigQuery.getMovieRatingsSchema, WRITE_TRUNCATE)

		sc.close()
		()
	}
}
