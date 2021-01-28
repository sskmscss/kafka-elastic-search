package com.tcs.service.edt

import com.tcs.service.edt.service.RxBus
import khttp.post
import org.json.JSONObject
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.ConfigurableApplicationContext
import java.text.SimpleDateFormat
import java.util.*

@SpringBootApplication(scanBasePackages = ["com.tcs.service.edt", "com.tcs.integration.common"])

class EventDrivenTestApplication

fun main(args: Array<String>) {
	val ctx: ConfigurableApplicationContext = runApplication<EventDrivenTestApplication>(*args)
	// Listen for String events only
	RxBus.listen(JSONObject::class.java).subscribe {
		try {
			when (it.optString("type")) {
				"kafka" -> {
					val obj = JSONObject(JSONObject(it.optString("data")).optString("headers"))
					val regex = "\\.([A-Za-z]+)$".toRegex()
					val matchResult = regex.find(obj.getString("event-aggregate-type"))
					val (eventName) = matchResult!!.destructured
					val url = ctx.environment.getProperty("cm.int.elastic-search.url") + obj.getString("PARTITION_ID") + "-" + eventName

					try {
						val df = SimpleDateFormat("EE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH)
						df.timeZone = TimeZone.getTimeZone("GMT")
						val date = df.parse(obj.getString("DATE"))
						val newDate = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
						val requestObject = mapOf(
								"@timestamp" to newDate.format(df.parse(obj.getString("DATE"))),
								"event_type" to eventName,
								"shipment_id" to obj.getString("PARTITION_ID"),
								"message" to it.optString("data")
						)
						val response = post(
								url = url,
								headers = mapOf("Content-Type" to "application/json"),
								json = requestObject)
					} catch(e: Exception) {
						println("Elastic search Exception :: $e")
					}
				}
			}
		} catch(e: Exception) {
			println(e)
		}
	}
}
