$(document).ready(

);

function submitUAFilter() {
	var useractivity = $("#activitytype").find("option:selected").attr("value");
	var topn = $("#topn").find("option:selected").attr("value");
	var daterange = $("#daterange").find("option:selected").attr("value");
	var urlString = "useractivity=" + useractivity + "&action=submitUAFilter"
			+ "&topn=" + topn + "&daterange=" + daterange;
	$
			.ajax({
				type : "post",
				url : '/channel',
				dataType : "text",
				cache : false,
				async : true,
				data : urlString,
				error : function() {
					alert("Status: " + textStatus + ". Response:"
							+ xhr.responseText
							+ "Error occurred while retrieving the data."
							+ xhr.status);
				},
				success : function(htmlResponse) {
					if (htmlResponse != '') {
						channelVideoDataParser(htmlResponse);
					}
				}
			});
}

function channelVideoDataParser(htmlResponse) {

}
