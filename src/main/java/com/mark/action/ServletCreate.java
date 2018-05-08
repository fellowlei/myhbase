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
@WebServlet(name = "ServletCreate",urlPatterns = "/HbaseServletCreate")
public class ServletCreate extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request,response);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException{
        String tname = request.getParameter("tablename");
        //多个familyName 用逗号分隔
        String familyName = request.getParameter("familyname");
        String msg = "-";
        try{
            if(HBaseUtils.create(tname,familyName)){
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