package mr.kpi;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class KPI {
	private String remote_addr;// 记录客户端的ip地址
    private String remote_user;// 记录客户端用户名称,忽略属性"-"
    private String time_local;// 记录访问时间与时区
    private String request;// 记录请求的url与http协议
    private String status;// 记录请求状态；成功是200
    private String body_bytes_sent;// 记录发送给客户端文件主体内容大小
    private String http_referer;// 用来记录从那个页面链接访问过来的
    private String http_user_agent;// 记录客户浏览器的相关信息
    private boolean valid = true; //验证每条数据是否有效
    
    
    public static KPI parser(String line) {
    	KPI kpi = new KPI();
    	String[] arr = line.split(" ");
    	if(arr.length > 11) {
    		kpi.setRemote_addr(arr[0]);
        	kpi.setRemote_user(arr[1]);
        	kpi.setTime_local(arr[3].substring(1));
        	kpi.setRequest(arr[6]);
        	kpi.setStatus(arr[8]);
        	kpi.setBody_bytes_sent(arr[9]);
        	kpi.setHttp_referer(arr[10]);
        	if(arr.length > 12) {
            	kpi.setHttp_user_agent(arr[11] + " " + arr[12]);
        	}else {
        		kpi.setHttp_user_agent(arr[11]);
        	}
        	if(Integer.parseInt(kpi.getStatus()) > 400) {
        		kpi.setValid(false);
        	}
    	}else {
    		kpi.setValid(false);
    	}
    	return kpi;
    }

    //按page的PV分类
    public static KPI filterPVs(String line) {
    	KPI kpi = parser(line);
    	Set<String> pages = new HashSet<String>();
        pages.add("/about");
        pages.add("/black-ip-list/");
        pages.add("/cassandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-zookeeper-intro/");
        pages.add("/hadoop-mahout-roadmap/");
        if(!pages.contains(kpi.getRequest())){
        	kpi.setValid(false);
        }
        return kpi;
    }
    
    
    
    public static void main(String[] args) {
    	String line = "60.208.6.156 - - [18/Sep/2013:06:49:48 +0000] \"GET /wp-content/uploads/2013/07/rcassandra.png HTTP/1.0\" 200 185524 \"http://cos.name/category/software/packages/\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36\"";
    	KPI kpi = new KPI();
    	String[] arr = line.split(" ");
    	kpi.setRemote_addr(arr[0]);
    	kpi.setRemote_user(arr[1]);
    	kpi.setTime_local(arr[3].substring(1));
    	kpi.setRequest(arr[6]);
    	kpi.setStatus(arr[8]);
    	kpi.setBody_bytes_sent(arr[9]);
    	kpi.setHttp_referer(arr[10]);
    	kpi.setHttp_user_agent(arr[11] + " " + arr[12]);
    	System.out.println(kpi);
    }

    @Override
    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	sb.append("valid:" + this.valid);
    	sb.append("\nremont_addr:" + this.remote_addr);
        sb.append("\nremote_user:" + this.remote_user);
        sb.append("\ntime_local:" + this.time_local);
        sb.append("\nrequest:" + this.request);
        sb.append("\nstatus:" + this.status);
        sb.append("\nbody_bytes_sent:" + this.body_bytes_sent);
        sb.append("\nhttp_referer:" + this.http_referer);
        sb.append("\nhttp_user_agent:" + this.http_user_agent);
    	return sb.toString();
    }
    
	String getRemote_addr() {
		return remote_addr;
	}


	void setRemote_addr(String remote_addr) {
		this.remote_addr = remote_addr;
	}


	String getRemote_user() {
		return remote_user;
	}


	void setRemote_user(String remote_user) {
		this.remote_user = remote_user;
	}


	String getTime_local() {
		return time_local;
	}


	void setTime_local(String time_local) {
		this.time_local = time_local;
	}

	Date getTime_local_date() throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.CHINA);
		return sdf.parse(this.time_local);
	}
	
	String getTime_local_date_hour() throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		return sdf.format(getTime_local_date());
	}

	String getRequest() {
		return request;
	}


	void setRequest(String request) {
		this.request = request;
	}


	String getStatus() {
		return status;
	}


	void setStatus(String status) {
		this.status = status;
	}


	String getBody_bytes_sent() {
		return body_bytes_sent;
	}


	void setBody_bytes_sent(String body_bytes_sent) {
		this.body_bytes_sent = body_bytes_sent;
	}


	String getHttp_referer() {
		return http_referer;
	}


	void setHttp_referer(String http_referer) {
		this.http_referer = http_referer;
	}


	String getHttp_user_agent() {
		return http_user_agent;
	}


	void setHttp_user_agent(String http_user_agent) {
		this.http_user_agent = http_user_agent;
	}


	boolean isValid() {
		return valid;
	}


	void setValid(boolean valid) {
		this.valid = valid;
	}
    
    
}
