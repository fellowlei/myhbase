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
 * https://blog.csdn.net/wild46cat/article/details/53306621
 */
@WebServlet(name = "ServletPut",urlPatterns = "/HbaseServletPut")
public class ServletPut extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request,response);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String tname = request.getParameter("tablename");
        String row = request.getParameter("row");
        String familyName = request.getParameter("familyname");
        String qualifier = request.getParameter("qualifier");
        String data = request.getParameter("data");
        String msg = "-";
        try{
            if(HBaseUtils.put(tname,row,familyName,qualifier,data)){
                msg="success";
            }else{
                msg="error";
            }
        }catch (Exception e){
            e.printStackTrace();
        }


        response.setCharacterEncoding("utf-8");
        response.setContentType("text/html;charset=utf-8");
        PrintWriter out = response.getWriter();
        out.println(msg);
        out.flush();
        out.close();
    }
}  