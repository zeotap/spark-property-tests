package com.zeotap.utility.spark.generator

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random

object RandomDataGenerator {

  def age(count: Int, start: Int = 12, end: Int = 100): List[String] = {
    List.fill(count)(start + Random.nextInt((end - start) + 1)).map(x => x.toString)
  }

  def UUID(count: Int): List[String] = {
    List.fill(count)(java.util.UUID.randomUUID().toString)
  }

  def OTR(count: Int): List[String] = {
    List.fill(count)((Random.nextDouble() * 100).ceil.toString())
  }

  def bundleid(count: Int): List[String] = {
    val values = List("droom.sleepIfUCan", "com.Deven.Arrow3D", "com.grindrapp.android", "net.zedge.android", "call.recorder.automatic.acr",
      "com.rubygames.assassin", "997362197", "com.pixel.art.coloring.color.number", "com.mobisystems.msdict.embedded.wireless.svcon.tlen.full",
      "com.lyrebirdstudio.collage", "com.milleniumapps.freealarmclock", "bp.free.puzzle.game.mahjong.onet", "art.color.planet.paint.by.number.game.puzzle.free",
      "com.bandagames.mpuzzle.gp", "com.nextwave.wcc_lt", "short.video.app", "com.crazylabs.acrylic.nails", "cn.wps.moffice_eng", "com.best.lucky.forecast",
      "com.easybrain.nonogram", "1533397036", "io.voodoo.crowdcity", "kik.android", "com.hideitpro", "com.ohmgames.cheatandrun", "com.smule.singandroid",
      "com.hld.anzenbokusucal", "fast.phone.clean", "com.thinkyeah.galleryvault", "multi.parallel.dualspace.cloner")
    text(count, values)
  }

  def rawIAB(count: Int): List[String] = {
    val values = List("IAB20_14", "IAB19", "IAB19,IAB19,IAB19,IAB19,IAB1_5", "IAB19,IAB19_47", "IAB19_18,IAB1_5,IAB1_5",
      "IAB19_9", "IAB19,IAB19,IAB19,IAB10", "IAB2_1,IAB16_3,IAB16_3", "IAB19_9,IAB19_56,IAB19")
    text(count, values)
  }

  def text(count: Int, values: List[String]): List[String] = {
    Random.shuffle(values).take(count)
  }

  def appCategory(count: Int): List[String] = {
    val values = List("Entertainment", "Games", "Social", "News")
    text(count, values)
  }

  def date(count: Int): List[String] = {
    val jdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    timestamp(count).map(x => {
      jdf.format(new Date(x.toLong * 1000L))
    })
  }

  def timestamp(count: Int): List[String] = {
    List.fill(count)((System.currentTimeMillis / 1000 - Random.nextInt(50000000)).toString)
  }

  def country(count: Int): List[String] = {
    val values = List("CAN", "USA", "MEX", "ITA", "FRA", "BGD", "DEU", "FIN", "POL", "COL", "CHL")
    text(count, values)
  }
}
