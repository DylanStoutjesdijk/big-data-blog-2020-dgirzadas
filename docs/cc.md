# Final project: Website decoration contest (Common crawl data)

Welcome! In this blog post, I will explain and discuss the work that I have done for the final project of the Big Data course (NWI-IBC036) at Radboud University.

## Introduction

We were given a free-form assignment to work with data from the [Common Crawl](https://commoncrawl.org/) dataset.

### The task

Have you ever wandered into a random website with an obscure domain name and were taken aback by a massive number of images and icons on the screen? Or maybe the opposite - disappointed by how dull and empty some ".com" websites are? I sure have. That is why I decided to analyse the data from the [Common Crawl](https://commoncrawl.org/) and see which domains have the most "extensively-decorated" sites.

## Part I: Set-up
Firstly, in order to perform any type of analysis, I needed to get my data. As the WARC (raw crawl data) dataset contains more than 50 TB of information, it was (by far) not possible to analyse the whole dataset locally.

However, I was able to prepare my computations for this cluster locally, by running tests on small (~1GB) subsets of the crawl dataset.

I obtained this subset (a subset from the [May/June 2020 crawl archive](https://commoncrawl.org/2020/06/may-june-2020-crawl-archive-now-available/)) with:

```bash
wget "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-24/segments/1590347385193.5/warc/CC-MAIN-20200524210325-20200525000325-00100.warc.gz"
```

To read these WARC files, I usedan implementation of Hadoop WarcReader in [`HadoopConcatGz`](https://github.com/helgeho/HadoopConcatGz)

```scala
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord
```

Firstly, to be able to do anything with the data, I had to initialise a new API Hadoop File in spark, which would store the necessary data types from the data that I downloaded:

```scala
val warcf = sc.newAPIHadoopFile(
              warcfile,
              classOf[WarcGzInputFormat],  // InputFormat
              classOf[NullWritable],       // Key
              classOf[WarcWritable]        // Value
    )
```

Now, this file contains a lot of information that is redundant to my analysis. To improve the runtime of the task, while also preventing multiple problems in the future, I filtered the dataset out to only contain information that is interesting for my analysis - the URLs of the web pages and their HTML contents:
```scala
val warc_filtered =                                                       //__
     warcs.map{ wr => wr._2 }.                                            //  |
        filter{ _.isValid() }.                                            //  |  
        map{ _.getRecord() }.                                             //  |- Taking valid pages with interesting content
        filter{ _.hasContentHeaders() }.                                  //  |
        filter{wr =>  wr.getHeaderValue("WARC-Type") == "response" }.     //__|
        map{wr => (wr.getHeader().getUrl(), wr.getHttpStringBody())}.     //   - Mapping these pages to simple (<URL>, <HTTP body>) pairs
        cache()                                                           //   - Caching in case of multiple analyses on the same set 
```

## Part II: Local analysis

Now that I had the data filtered and prepared, I could run my analysis.

First off, I wanted to know which **domains** (top-level) are the most "well-decorated". This means that I needed to parse the top-level domain name (e.g. ".com") from the full URL. To do that, I used the trusty ol' regular expression:
```scala
val hostnamePattern = """^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)""".r
```
And a matcher function:
```scala
def getHostname(text: String): Option[String] = for (m <- hostnamePattern.findFirstMatchIn(text)) yield m.group(1)
```
This regular expression has been taken from [stack overflow](https://stackoverflow.com/questions/25703360/regular-expression-extract-subdomain-domain).
Now, this expression captures the whole host name (e.g. "https://play.google.com"), but it is easy to parse the top-level from it by taking whatever is after the last `"."` and the full host name will come in handy later ;).

### Domain image counts
To compute how many images each top-level domain contains, I did the following:
1. Re-map the keys (full URLs) to only top-level domain names.
2. Map the values to "1" for each image present in the web page.
    - The "img" tags are parsed with `Jsoup.parse().select()` method. 
4. Reduce these mapped values by key - aggregate all "1"s for each top-level domain.
5. Sort these resulting `(<top-level domain>, <image count>)` pairs so the domains with most images are on top.

This process is translated into code as follows:

```scala
val domain_imageCounts = warcok.                                           //__
            map{wr => (StringUtils.substring(                              //  |
                getHostname(wr._1).fold("")(_.toString),                   //  |- Key => Extracted top-level domain name
                getHostname(wr._1).fold("")(_.toString).lastIndexOf("."),  //  |
                getHostname(wr._1).fold("")(_.toString).length())          //__|
                , wr._2)}.                                                 //   - Value => HTTP content of the page
            flatMapValues{http =>                       
            Jsoup.parse(http).select("img[src]").asScala.        
            map{img => 1}}.                                 // Mapping values to "1" for each "img" tag in the HTML
            reduceByKey((a, b) => a + b).                   // Aggregating all the "1"s for all top-level domains
            sortBy(_._2, false).                            // Putting the domains with most images at the top
            cache()                                         // Caching for later use
```

After running this code (takes around 3 minutes on the previously mentioned small CC data subset), I got the following results:

```
(.com,535692)
(.ru,55590)
(.net,49601)
(.org,43748)
(.de,38673)
```

WOW! The `.com` domain is a clear winner here, right? Not so fast. The subset that I am analysing most likely consists of data mostly from `.com` pages, as this top-level domain is so prominent. 

### Domain web page counts

So, I need to normalise the results.

To do that, I need to get the total counts of websites from each top-level domain. This is done in a very similar manner as image counting, but a little more simple:

```scala
val domain_siteCounts = warcok.                                                      //__
                        map{wr => (StringUtils.substring(                            //  |         
                            getDomain(wr._1).fold("")(_.toString),                   //  |- Key => Extracted top-level domain name       
                            getDomain(wr._1).fold("")(_.toString).lastIndexOf("."),  //  |             
                            getDomain(wr._1).fold("")(_.toString).length())          //__|        
                            , 1)}.                                                   //   - Value => "1"
                        reduceByKey((a, b) => a + b).  // Aggregating all the "1"s for all top-level domains
                        sortBy(_._2, false).           // Putting the domains with most pages at the top                            
                        cache()                        // Caching for later use
```

So, by running this code (around 1.5 minutes), we get the results:

```
(.com,21236)
(.org,2607)
(.ru,2116)
(.de,1849)
(.net,1806)
```

Yup, as expected, there are way more `.com` web pages than others. Also, all of these top 5 top-level domains appeared in the "most images" list, so I expect to see some heavy normalisation effects.

So, to normalise, I just divide the image counts by the site counts. To do that, I joined the two RDDs (only took top-level domains that have more than 250 web pages to their name) and divided the numbers:
```scala
val relative_imageCounts = domain_siteCounts.
                        filter(_._2 > 250).                       // Only take domains with more than 250 pages
                        join(domain_imageCounts).                 // Join the image count RDD to the page count one
                        mapValues{x =>
                        (x._2.toDouble / x._1.toDouble,           // First result value is the ratio images/sites
                        s"${x._2} images in ${x._1} websites")}.  // Second is the explanation of this ratio
                        sortBy(_._2._1, false).                   // Put the domains with most images per page on top
                        cache()                                   // Cache for repeated use
```

Running this code didn't take longer than a second, as I had already cached the two RDDs that were joined.

So the final results!
The TOP-10 of the most "well-decorated" top-level domains is:

```
(.pl,(33.34530386740332,24142 images in 724 websites))
(.jp,(31.226779252110976,25887 images in 829 websites))
(.info,(29.51121076233184,13162 images in 446 websites))
(.net,(27.464562569213733,49601 images in 1806 websites))
(.ru,(26.27126654064272,55590 images in 2116 websites))
(.ua,(26.11111111111111,9635 images in 369 websites))
(.com,(25.225654548879263,535692 images in 21236 websites))
(.br,(24.180981595092025,15766 images in 652 websites))
(.es,(22.104368932038835,9107 images in 412 websites))
(.de,(20.915630070308275,38673 images in 1849 websites))
```

Congratulations, Poland! Your websites have the most images in them! (At least in this small subset of the web crawl).

### Honourable mentions

Well, Poland got the gold. However, I can not leave out the honourable mentions - the web pages that were out of their league. Let's look at the pages that had the most images in one page.

This has been achieved in basically the same way as top-level domain image counts, but with full URLs as keys instead:

```scala
val site_imageCounts = warcok.                                            
            map{wr => 
            (wr._1,                                         // Key => URL, 
            wr._2)}.                                        // Value => HTTP content of the page
            flatMapValues{http =>                       
            Jsoup.parse(http).select("img[src]").asScala.        
            map{img => 1}}.                                 // Mapping values to "1" for each "img" tag in the HTML
            reduceByKey((a, b) => a + b).                   // Aggregating all the "1"s for all URLs
            sortBy(_._2, false).                            // Putting the URLs with most images at the top
            cache()                                         // Caching for later use
```

After running the code for around 3 minutes, I got the list of the websites with most images. This list contained a wide variety of sites, such as a gallery page from a primary school in Latvia or a gallery page of a Dutch charity association (images for the photo thumbnails) but also obscure mobile app sites (images with app logos) and even a Japanese adult chatroom site with a bunch of incorrectly-scaled banners and thumbnails (of course, this is the Internet after all...) - All of them with more than 1000 images in one page!

