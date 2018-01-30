package com.up.cardholderTag.hbase.test;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.client.Result;

/**
 * Servlet implementation class LoginServlet
 */
public class LoginServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public LoginServlet() {
        super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String usrNm = request.getParameter("usrNm");
		String pwd = request.getParameter("pwd");
		String quitFlag = request.getParameter("quitFlag");
		String servletFlag="";
		String loginFlag="";
		
		if(quitFlag!=null && quitFlag.equals("1")) {
			System.out.println("LoginServlet: quitFlag: " + quitFlag);
			request.setAttribute("quitFlag", quitFlag);
		} else {
			HBaseService hbs = new HBaseService();
			LoginModel model = new LoginModel();
			Result res = hbs.get("tbl_cardholder_sys_user", "yjy-01", "d");
			model = hbs.getLoginModel(res);
			
			if (model == null) {
				return;
			}
			
			if(pwd!=null && pwd.equals(model.getUserPwd()) /*&& ins.equals(model.getInsCd())*/) {
				servletFlag = "1";
				loginFlag = "1";
			} else {
				servletFlag = "1";
				loginFlag = "0";
			}
			
			System.out.println("LoginServlet: servletFlag: " + servletFlag);
			System.out.println("LoginServlet: loginFlag: " + loginFlag);
			
			request.setAttribute("servletFlag", servletFlag);
			request.setAttribute("loginFlag", loginFlag);
		}
		
		request.getRequestDispatcher("/jsp/login.jsp").forward(request,
				response);
	}
}
