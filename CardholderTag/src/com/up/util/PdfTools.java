package com.up.util;
import java.io.FileOutputStream;
import java.io.IOException;

import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Element;
import com.itextpdf.text.Font;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.BaseFont;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;


public class PdfTools {
 /**
  *
  * 生成PDF的方法
  *
  * @return boolean
  *
  */

 public static boolean createPDF(String pdfPath) {

  Document document = new Document();// 建立一个Document对象

  document.setPageSize(PageSize.A4);// 设置页面大小

  try {

   PdfWriter.getInstance(document, new FileOutputStream(pdfPath));// 建立一个PdfWriter对象
   document.open();
   //BaseFont bfChinese = BaseFont.createFont("STSong-Light",
   //  "UniGB-UCS2-H", BaseFont.NOT_EMBEDDED);// 设置中文字体
   BaseFont bfChinese = BaseFont.createFont("D://ReportFactory//util//fonts//simsun.ttc,1" , BaseFont.IDENTITY_H, BaseFont.EMBEDDED);// 设置中文字体
   
   Font titleFont = new Font(bfChinese, 15, Font.BOLD);// 设置字体大小
   Font headFont = new Font(bfChinese, 10, Font.BOLD);// 设置字体大小
   Font headFont1 = new Font(bfChinese, 10, Font.BOLD);// 设置字体大小
   Font headFont2 = new Font(bfChinese, 10, Font.NORMAL);// 设置字体大小
   document.add(new Paragraph("标题",
     headFont));
   
   float[] widths = { 140f, 60f, 320f, 120f, 110f, 110f, 190f };// 设置表格的列宽
   
   PdfPTable table = new PdfPTable(widths);// 建立一个pdf表格

   table.setSpacingBefore(20f);// 设置表格上面空白宽度

   table.setTotalWidth(535);// 设置表格的宽度

   table.setLockedWidth(true);// 设置表格的宽度固定

    table.getDefaultCell().setBorder(1);//设置表格默认为边框1

   PdfPCell cell = new PdfPCell(new Paragraph("Taony125 testPdf 中文字体",
     headFont));// 建立一个单元格

   // cell.setBorder(0);//设置单元格无边框

//   cell.setColspan(7);// 设置合并单元格的列数

   table.addCell(cell);// 增加单元格

   cell = new PdfPCell(
     new Paragraph("Taony125 testPdf 中文字体", headFont));

   // cell.setBorder(0);

   cell.setFixedHeight(20);

   cell.setColspan(7);// 设置合并单元格的列数

   cell.setHorizontalAlignment(Element.ALIGN_CENTER);// 设置内容水平居中显示

   cell.setVerticalAlignment(Element.ALIGN_MIDDLE);

   table.addCell(cell);

   cell = new PdfPCell(new Paragraph("Taony125 testPdf 中文字体",
     headFont1));

   // cell.setBorder(0);

   cell.setFixedHeight(20);

//   cell.setColspan(7);// 设置合并单元格的列数

   cell.setHorizontalAlignment(Element.ALIGN_CENTER);// 设置内容水平居中显示

   cell.setVerticalAlignment(Element.ALIGN_MIDDLE);
   
   table.addCell(cell);

   table.addCell(new Paragraph("Taony125 testPdf 中文字体", headFont2));

   document.add(table);

  } catch (DocumentException de) {

   System.err.println(de.getMessage());

   return false;

  }

  catch (IOException ioe) {

   System.err.println(ioe.getMessage());

   return false;

  }

  document.close();

  return true;

 }

 /**
  *
  * @param args
  *
  */
 public static void main(String[] args) {

  // TODO 自动生成方法存根

  PdfTools.createPDF("d:/test.pdf");

 }

}