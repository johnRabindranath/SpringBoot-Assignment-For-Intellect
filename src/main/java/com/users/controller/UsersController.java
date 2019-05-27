package com.users.controller;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.users.model.User;

@RestController
public class UsersController {
	ObjectMapper mapper = new ObjectMapper();

	private TransportClient esclient;

	@Value("${elasticsearch.host}")
	private String elasticSearchHost;

	@Value("${elasticsearch.cluster}")
	private String elasticSearchcluster;

	/**
	 * create the user
	 * 
	 * @param user
	 * @return
	 */
	@PostMapping("/createNewUser")
	public String createNewUser(@RequestBody User user) {
		System.setProperty("es.set.netty.runtime.available.processors", "false");

		esclient = connectESClient();
		BulkRequestBuilder bulkRequests = esclient.prepareBulk();
		String userMessage = "";
		try {

			SearchResponse totalResponse = esclient.prepareSearch("users").setTypes("user")
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.matchAllQuery())
					.setSize(5000).setExplain(true).get();
			SearchHits totalData = totalResponse.getHits();
			if (totalData.getTotalHits() != 0) {

				SearchResponse response = esclient.prepareSearch("users").setTypes("user")
						.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
						.setQuery(QueryBuilders.termQuery("email.keyword", user.getEmail())).setSize(5000)
						.setExplain(true).get();
				SearchHits hitsdata = response.getHits();
				System.out.println("Result" + hitsdata.getTotalHits());
				if (hitsdata.getTotalHits() == 0) {
					User userData = new User();
					int maxsqid = 1;
					SearchResponse responsess = esclient.prepareSearch("users").setTypes("user")
							.setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.matchAllQuery())
							.setSize(5000).addSort("_id", SortOrder.DESC).setExplain(true).get();
					SearchHits hitss = responsess.getHits();
					if (hitss.getTotalHits() != 0) {
						for (SearchHit hit : hitss) {
							try {
								userData = mapper.readValue(hit.getSourceAsString(), User.class);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							if (userData != null) {
								maxsqid = Integer.parseInt(hit.getId()) + 1;
								System.out.println("maxId" + maxsqid);
							}
							break;
						}
						User userDatas = new User();
						userDatas.setfName(user.getfName());
						userDatas.setlName(user.getlName());
						userDatas.setEmail(user.getEmail());
						userDatas.setPinCode(user.getPinCode());
						Date todayDate = new Date();
						if (user.getBirthDate().before(todayDate)) {
							userDatas.setBirthDate(user.getBirthDate());
						}
						String stringifiedJson = mapper.writeValueAsString(userDatas);
						bulkRequests.add(esclient.prepareIndex("users", "user", String.valueOf(maxsqid))
								.setSource(stringifiedJson, XContentType.JSON));
						if (bulkRequests.request().requests().size() != 0) {
							BulkResponse bulkResponse = bulkRequests.execute().actionGet();
							if (bulkResponse.getItems() != null) {
								userMessage = "User is Created" + " " + maxsqid;
							} else if (bulkResponse.hasFailures()) {
								userMessage = "User Is Not Created";

							}
						}
					}
				}

				else {
					userMessage = "Already the user is there";
				}

			} else {
				User userDatas = new User();
				userDatas.setfName(user.getfName());
				userDatas.setlName(user.getlName());
				userDatas.setEmail(user.getEmail());
				userDatas.setPinCode(user.getPinCode());
				userDatas.setBirthDate(user.getBirthDate());
				String stringifiedJson = mapper.writeValueAsString(userDatas);
				bulkRequests.add(esclient.prepareIndex("users", "user", String.valueOf(1)).setSource(stringifiedJson,
						XContentType.JSON));
				if (bulkRequests.request().requests().size() != 0) {
					BulkResponse bulkResponse = bulkRequests.execute().actionGet();
					if (bulkResponse.getItems() != null) {
						userMessage = "User is Created" + " " + 1;
					} else if (bulkResponse.hasFailures()) {
						userMessage = "User Is Not Created";

					}
				}
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return userMessage;
	}

	/**
	 * update the user based on the id
	 * 
	 * @param user
	 * @param id
	 * @return
	 */
	@PutMapping("/updateUser")
	public String updateUser(@RequestBody User user) {
		esclient = connectESClient();
		BulkRequestBuilder bulkRequests = esclient.prepareBulk();
		String userMessage = "";
		try {

			SearchResponse response = esclient.prepareSearch("users").setTypes("user")
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.matchQuery("_id", user.getId()))
					.setSize(500).setExplain(true).get();
			SearchHits hitsdata = response.getHits();
			System.out.println("Result" + hitsdata.getTotalHits());
			if (hitsdata.getTotalHits() != 0) {
				for (SearchHit searchHit : hitsdata) {
					User userData = mapper.readValue(searchHit.getSourceAsString(), User.class);
					if (userData != null) {
						userData.setPinCode(user.getPinCode());
						userData.setBirthDate(user.getBirthDate());
						String stringifiedJson = mapper.writeValueAsString(userData);
						bulkRequests.add(esclient.prepareIndex("users", "user", String.valueOf(user.getId()))
								.setSource(stringifiedJson, XContentType.JSON));
						if (bulkRequests.request().requests().size() != 0) {
							BulkResponse bulkResponse = bulkRequests.execute().actionGet();
							if (bulkResponse.getItems() != null) {
								userMessage = "User Data Is Updated";
							} else if (bulkResponse.hasFailures()) {
								userMessage = "User Data Is Not Updated";

							}
						}

					}

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return userMessage;
	}
	/**
	 * Delete the existing user based on userid Do not delete the record only
	 * deactivate the user
	 */
	@PutMapping("/deleteUser")
	public String deleteUser(@RequestBody User user) {

		esclient = connectESClient();
		BulkRequestBuilder bulkRequests = esclient.prepareBulk();
		String userMessage = "";
		try {

			SearchResponse response = esclient.prepareSearch("users").setTypes("user")
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
					.setQuery(QueryBuilders.matchQuery("_id", user.getId())).setSize(500).setExplain(true).get();
			SearchHits hitsdata = response.getHits();
			System.out.println("Result" + hitsdata.getTotalHits());
			if (hitsdata.getTotalHits() != 0) {
				for (SearchHit searchHit : hitsdata) {
					User userData = mapper.readValue(searchHit.getSourceAsString(), User.class);
					if (userData != null) {
						userData.setIsActive(false);
						String stringifiedJson = mapper.writeValueAsString(userData);
						bulkRequests.add(esclient.prepareIndex("users", "user", String.valueOf(user.getId()))
								.setSource(stringifiedJson, XContentType.JSON));
						if (bulkRequests.request().requests().size() != 0) {
							BulkResponse bulkResponse = bulkRequests.execute().actionGet();
							if (bulkResponse.getItems() != null) {
								userMessage = "Is Active  flag is updated to false";
							} else if (bulkResponse.hasFailures()) {
								userMessage = "Is Active flag is not updated to false";

							}
						}

					}

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return userMessage;

	}

	/**
	 * Get the users whose birthdayFails in current month
	 * 
	 * @return
	 */
	@GetMapping("/birthdayFalls")
	public List<User> birthdayFalls() {
		esclient = connectESClient();

		ArrayList<User> birthdayFalls = new ArrayList<>();
		try {
			Date currentDate = new Date();
			SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
			String strDate = formatter.format(currentDate);
			System.out.println(strDate);

			Calendar c = Calendar.getInstance(); // this takes current date
			c.set(Calendar.DAY_OF_MONTH, 1);
			SimpleDateFormat formatterr = new SimpleDateFormat("dd-MM-yyyy");
			String strDates = formatterr.format(c.getTime());

			System.out.println(strDates);

			SearchResponse response = esclient.prepareSearch("users").setTypes("user")
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
					.setQuery(QueryBuilders.rangeQuery("birthDate").gte(strDates).lte(strDate)).setSize(500)
					.setExplain(true).get();

			SearchHits hitsdata = response.getHits();
			System.out.println("Result" + hitsdata.getTotalHits());
			if (hitsdata.getTotalHits() != 0) {

				for (SearchHit searchHit : hitsdata) {
					User userData = mapper.readValue(searchHit.getSourceAsString(), User.class);
					birthdayFalls.add(userData);

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return birthdayFalls;

	}

	/**
	 * Get monthwise count of birthdays
	 * 
	 * @return
	 * @return
	 */
	@GetMapping("/monthwiseCount")
	public HashMap<String, Integer> monthwiseCount() {
		esclient = connectESClient();

		HashMap<String, Integer> monthwiseCount = new HashMap<String, Integer>();
		try {

			for (int i = 0; i <= 11; i++) {
				Calendar calendar = Calendar.getInstance();
				calendar.set(Calendar.YEAR, 2019);
				calendar.set(Calendar.MONTH, i);

				calendar.set(Calendar.DATE, calendar.getActualMinimum(Calendar.DAY_OF_MONTH));
				Date nextMonthFirstDay = calendar.getTime();
				SimpleDateFormat formatterr = new SimpleDateFormat("dd-MM-yyyy");
				String monthFirstDay = formatterr.format(nextMonthFirstDay);
				System.out.println("monthFirstDay" + monthFirstDay);
				SimpleDateFormat monthName = new SimpleDateFormat("MMMM"); // full month name
				String month = monthName.format(nextMonthFirstDay);

				calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
				Date nextMonthLastDay = calendar.getTime();
				SimpleDateFormat formatData = new SimpleDateFormat("dd-MM-yyyy");
				String monthLastDay = formatData.format(nextMonthLastDay);
				System.out.println("monthFirstDay" + monthLastDay);

				SearchResponse response = esclient.prepareSearch("users").setTypes("user")
						.setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.rangeQuery("birthDate")
								.from(monthFirstDay).to(monthLastDay).includeLower(true).includeUpper(false))
						.setSize(500).setExplain(true).get();
				SearchHits hitsdata = response.getHits();

				System.out.println("Query" + QueryBuilders.rangeQuery("birthDate").from(monthFirstDay).to(monthLastDay)
						.includeLower(true).includeUpper(false));

				System.out.println("Result" + hitsdata.getTotalHits());
				int count = (int) hitsdata.getTotalHits();

				if (hitsdata.getTotalHits() != 0) {
					monthwiseCount.put(month, count);

				} else {
					monthwiseCount.put(month, count);

				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return monthwiseCount;

	}

	/**
	 * Connecting local Es
	 * 
	 * @return
	 */
	@SuppressWarnings("resource")
	public TransportClient connectESClient() {

		TransportClient esClient = null;
		try {
			Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
			esClient = new PreBuiltTransportClient(settings)
					.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
		} catch (UnknownHostException e) {
		}
		return esClient;
	}

}
