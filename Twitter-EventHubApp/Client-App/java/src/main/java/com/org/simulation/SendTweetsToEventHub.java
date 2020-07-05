package com.org.simulation;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class SendTweetsToEventHub {

	// Replace values below with real configurations
	final private static String twitterConsumerKey = "19boShujUPEf88PoYWeYVZp33";
	final private static String twitterConsumerSecret = "QNEMkVurwpgcaTo3zQGsszfc1DYYTepnEpkX63CCOIDfMbVLQU";
	final private static String twitterOauthAccessToken = "3031481257-9PZis7AVEf78eV74c1l2kvYe6rFuANZiBt0xfF9";
	final private static String twitterOauthTokenSecret = "w7ktYZlBZmULObXTKSSaYCinDnIo0Qva1VH6OEvpzSsbI";
	
	final private static String namespaceName = "eventhubnamespacesample";
	final private static String eventHubName = "eventhubsample";
	final private static String sasKeyName = "manage";
	final private static String sasKey = "+bBOu6BJ9v/VX+p5ZlK7GapXs27nml4XE+UpZGjBpAA=";
	private static CompletableFuture<EventHubClient> eventHubClient;

	public static void main(String[] args) 
			throws TwitterException, EventHubException, IOException, InterruptedException, ExecutionException {

		 // EventHub configuration!
		ConnectionStringBuilder connStr = new ConnectionStringBuilder()
				.setNamespaceName(namespaceName)
				.setEventHubName(eventHubName)
				.setSasKeyName(sasKeyName)
				.setSasKey(sasKey);

		ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);
		eventHubClient = EventHubClient.create(connStr.toString(), pool);


		 // Twitter configuration!
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		.setOAuthConsumerKey(twitterConsumerKey)
		.setOAuthConsumerSecret(twitterConsumerSecret)
		.setOAuthAccessToken(twitterOauthAccessToken)
		.setOAuthAccessTokenSecret(twitterOauthTokenSecret);

		TwitterFactory twitterFactory = new TwitterFactory(cb.build());
		Twitter twitter = twitterFactory.getInstance();

		// Getting tweets with keyword "Azure" and sending them to the Event Hub in realtime!
		Query query = new Query(" #Azure ");
		query.setCount(100);
		query.lang("en");
		boolean finished = false;
		while (!finished) {
			QueryResult result = twitter.search(query);
			List<Status> statuses = result.getTweets();
			long lowestStatusId = Long.MAX_VALUE;
			for(Status status:statuses){
				if(!status.isRetweet()){
					sendEvent(status.getText(), 5000);
				}
				lowestStatusId = Math.min(status.getId(), lowestStatusId);
			}
			query.setMaxId(lowestStatusId - 1);
		}

	}

	private static void sendEvent(String message, long delay) throws InterruptedException, UnsupportedEncodingException, ExecutionException {
		Thread.sleep(delay);
		EventData messageData = EventData.create(message.getBytes("UTF-8"));
		eventHubClient.get().send(messageData);
		System.out.println("Sent event: " + message + "\n");

	}

}
