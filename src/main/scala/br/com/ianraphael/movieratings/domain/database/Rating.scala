package br.com.ianraphael.movieratings.domain.database

case class Rating (
  movieId: Int,
  score: Double,
  count: Long = 1l
)