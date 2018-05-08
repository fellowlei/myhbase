package com.mark.action;

import com.mark.hbase.HBaseUtils;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by Administrator on 2016-11-23.
 */
@WebServlet(name = "ServletDeleteQualifier",urlPatterns = "/HBaseServlertDeleteQualifier")
public class ServletDeleteQualifier extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request,response);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String tname = request.getParameter("tablename");
        String rowName = request.getParameter("rowname");
        String columnfamilynameName = request.getParameter("columnfamilyname");
        String qualifierName = request.getParameter("qualifiername");
        String msg = "-";
        if(HBaseUtils.deleteQualifier(tname,rowName,columnfamilynameName,qualifierName)){
            msg = "success";
        }else{
            msg = "error";
        }

        response.setCharacterEncoding("utf-8");
        response.setContentType("text/html;charset=utf-8");
        PrintWriter out = response.getWriter();
        out.println(msg);
        out.flush();
        out.close();
    }
}
