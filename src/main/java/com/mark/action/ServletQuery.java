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
 * Created by Administrator on 2016-11-24.
 * https://blog.csdn.net/wild46cat/article/details/53321409
 */
@WebServlet(name = "ServletQuery", urlPatterns = "/HBaseServletQueryEqual")
public class ServletQuery extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String tname = request.getParameter("tablename");
        String columnFamily = request.getParameter("columnfamily");
        String qualifier = request.getParameter("qualifier");
        String data = request.getParameter("data");
        String msg = "-";
        try {
            msg = HBaseUtils.queryequal(tname, columnFamily,qualifier,data);
        } catch (Exception e) {
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
