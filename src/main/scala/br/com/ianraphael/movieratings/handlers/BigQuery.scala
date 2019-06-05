package br.com.ianraphael.movieratings.handlers

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import collection.JavaConverters._

object BigQuery {
	def getMovieRatingsSchema: TableSchema = {
		val fields = List(
			new TableFieldSchema().setName("movie_id").setType("INTEGER"),
			new TableFieldSchema().setName("movie_name").setType("STRING"),
			new TableFieldSchema().setName("rating").setType("FLOAT"),
			new TableFieldSchema().setName("rating_count").setType("INTEGER")
		)

		new TableSchema().setFields(fields.asJava)
	}

	def getMovieRatingsTable: String = "cinema.movie_ratings"
}
