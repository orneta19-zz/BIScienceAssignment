import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.PairRDDFunctions

/** BIScience Assignment.
  * Created by Neta on 12/04/2016.
  */
object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("extractFromFile")
    val sc = new SparkContext(conf)

    val inputPath =  "../BIScience/DSinTab*" // I had changed the file to a .tsv, as it is easier to parse


    val BISCienceData = sc.textFile(inputPath)
      .map(x => {
        val replaced = x.replaceAll("\"", "") // replaced all apostrophes for easier comprehension
        (replaced)
      })

    val header = BISCienceData.first()

    val filterBIScienceData = BISCienceData
        .filter(x => x != header) // filtered out header, so it won't interfer with calculations
        .map(x => {
          val splitx = x.split("\t")
          splitx
        })


    /** 'popularAdTypeByGeo' shows the most popular ad types in different geographies.
      */
    val popularAdTypeByGeo = filterBIScienceData
      .map(x => {
        val adType = x(10)
        val occurrence = x(12).toInt
        val countryCode = x(1)

        ((countryCode, adType), occurrence)
      })
      .map(x => ((x._1._1, x._1._2), x._2))
      .reduceByKey((k,v) => (k + v)) // counts the times each ad type appears in each geo
      .sortBy(_._2, false) // sorts by desc order
      .map(x => (x._1._1, (x._1._2, x._2)))
      .groupByKey() // groups by geo
      .map(x => {
        val geoOccurences = x._2.map(x => x._2).reduce(_+_) // counts total times each geo appeared

        (x._1, geoOccurences, (x._2))
      })
      .sortBy(_._2, false) // sorts by desc order


    popularAdTypeByGeo.take(20).foreach(x => println(x))


    /** 'popChannelForAdvertByGeo' shows tbe most popular channels for different advertisers, based on geography.
      */
    val popChannelForAdvertByGeo = filterBIScienceData
      .map(x => {
        val channel = x(14)
        val advertiserName = x(3)
        val countryCode = x(1)

        ((countryCode, advertiserName, channel), 1)
      })
      .reduceByKey(_+_) // counts the times each advertiser used a specific channel
      .sortBy(_._2, false) // sorts by  desc order
      .map(x => {
        val countryCode = x._1._1
        val advertiserName = x._1._2
        val channel = x._1._3
        val channelCount = x._2

        ((countryCode, channel), (advertiserName, channelCount))
      })
      .groupByKey() // groups by geo and channel
      .map(x => {
        val countryCode = x._1._1
        val channel = x._1._2
        val totalGeoCount = x._2.map(x => x._2).reduce(_+_)

        (countryCode, (totalGeoCount, (channel, (x._2))))
      })
      .groupByKey() // groups by geo
      .sortBy(_._2, false) // sorts by desc order


    popChannelForAdvertByGeo.take(20).foreach(x => println(x))


    /** 'popPublishForAdvertByGeo' shows the most popular publishers for different advertisers, by geography
      */
    val popPublishForAdvertByGeo = filterBIScienceData
      .map(x => {
        val publisherDomain = x(2)
        val advertiserName = x(3)
        val countryCode = x(1)

        ((countryCode, publisherDomain, advertiserName), 1)
      })
      .reduceByKey(_+_) // counts the times each advertiser used a specific publisher, by geography
      .sortBy(_._2, false) // sorts by desc order
      .map(x => {
       val countryCode = x._1._1
       val publisherDomain = x._1._2
       val advertiserName = x._1._3
       val totUsesOfPubDomByAdvert = x._2

      ((countryCode, publisherDomain), (advertiserName, totUsesOfPubDomByAdvert))
      })
      .groupByKey() // groups by geo and publisher
      .map(x => {
        val countryCode = x._1._1
        val publisherDommain = x._1._2
        val advertiserAndCount = x._2
        val totalPublisherDomCount = x._2.map(x => x._2).reduce(_+_)

        (countryCode, (publisherDommain, totalPublisherDomCount, advertiserAndCount))
      })
      .groupByKey() // groups by geo
      .sortBy(_._2.map(x => x._3.map(x => x._2)), false) // sorts by desc order
      .sortBy(_._2.map(x => x._2), false) // sorts by desc order


    popPublishForAdvertByGeo.take(20).foreach(x => println(x))


    /** 'popMediatorForAdvertByGeo' shows the most popular mediators for different advertisers by geography.
      */
    val popMediatorForAdvertByGeo = filterBIScienceData
      .map(x => {
        val firstMediator = x(11)
        val advertiserName = x(3)
        val countryCode = x(1)

        ((countryCode, firstMediator, advertiserName), 1)
      })
      .reduceByKey(_+_) // counts the times each advertiser used a mediator, by geography
      .map(x => {
       val countryCode = x._1._1
       val firstMediator = x._1._2
       val advertiserName = x._1._3
       val totUseOfMediatorByAdvert = x._2

      ((countryCode, firstMediator), (advertiserName, totUseOfMediatorByAdvert))
    })
      .groupByKey() // groups by geo and mediator
      .sortBy(_._2.map(x => x._2), false) // sorts by desc order
      .map(x => {
       val countryCode = x._1._1
       val firstMediator = x._1._2
       val advertiserAndCount = x._2
       val totalMediatorCount = x._2.map(x => x._2).reduce(_+_)

      (countryCode, (firstMediator, totalMediatorCount, advertiserAndCount))
    })
      .groupByKey() // groups by geo
      .sortBy(_._2.map(x => x._2), false) // sorts by desc order
      .map(x => {
      val totalGeoCount = x._2.map(x => x._2).reduce(_+_)

      (x._1, totalGeoCount, x._2)
    })
    .sortBy(_._2, false) // sorts by desc order


    popMediatorForAdvertByGeo.take(20).foreach(x => println(x))
    
    sc.stop()
  }

}
