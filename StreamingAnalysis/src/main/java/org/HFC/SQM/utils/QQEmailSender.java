package org.HFC.SQM.utils;

import org.json.JSONObject;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class QQEmailSender {

    /**
     * 使用 QQ 邮箱发送邮件的方法
     *
     * @param username 发件人 QQ 邮箱用户名（完整邮箱地址）
     * @param authCode 发件人 QQ 邮箱的 SMTP 授权码
     * @param to       收件人邮箱地址
     * @param subject  邮件主题
     * @param content  邮件内容
     * @throws MessagingException 处理邮件发送过程中可能出现的异常
     */
    public static void sendQQEmail(String username, String authCode, String to, String cc, String subject, String content) throws MessagingException {
        // QQ 邮箱 SMTP 服务器配置
        String host = "smtp.qq.com";
        String port = "587";

        // 配置邮件服务器属性
        Properties properties = new Properties();
        properties.put("mail.smtp.host", host);
        properties.put("mail.smtp.port", port);
        properties.put("mail.smtp.auth", "true");
        properties.put("mail.smtp.starttls.enable", "true");

        // 创建会话对象
        Session session = Session.getInstance(properties, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, authCode);
            }
        });

        // 创建邮件消息
        Message message = new MimeMessage(session);
        message.setFrom(new InternetAddress(username));
        message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
        if (cc != null && !cc.isEmpty()) {
            message.setRecipients(Message.RecipientType.CC, InternetAddress.parse(cc));
        }
        message.setSubject(subject);
        message.setText(content);

        // 发送邮件
        Transport.send(message);
    }


    public static void errorNoteEmail(String flag, String info) throws MessagingException {
        JSONObject jsonObject = new JSONObject(info);
        Integer id = jsonObject.getInt("id");
        Integer down_time = jsonObject.getInt("down_time");
        int threshold = jsonObject.getInt("threshold");
        String subject;
        switch (threshold) {
            case 60:
                subject = "停机警告（1分钟）";
                break;
            case 300:
                subject = "严重停机警告（5分钟）";
                break;
            case 600:
                subject = "紧急停机警告（10分钟）";
                break;
            default:
                subject = "设备停机警告";
        }
        String username = "9***@qq.com";
        // 发件人 QQ 邮箱的 SMTP 授权码
        String authCode = "*******";
        // 邮件主题


        MySQLDatabaseConnector connector = new MySQLDatabaseConnector();
        String[] detail = connector.queryFactoryInfoById(id);
        String factoryName = detail[0];
        String workshopName = detail[1];
        String lineName = detail[2];
        String to = detail[3];
        String cc = detail[4];
        String content = "各位同事，\n" +
                "\n" +
                "请注意，我们的设备 [" + factoryName + " - " + workshopName + " - " + lineName + "] 于 " + down_time + " 检测到异常，现已自动停机。具体原因正在排查中。\n" +
                "\n" +
                "请相关人员暂停使用该设备，并避免靠近故障区域，等待进一步通知。\n" +
                "\n" +
                "感谢配合！\n" +
                "\n" +
                "[您的姓名]\n" +
                "[公司名称]\n" +
                "[日期]";
        if (flag == "True") {
            System.out.println(content);
        } else {   //            // 发件人 QQ 邮箱地址
            System.out.println(content);
//            sendQQEmail(username, authCode, to, cc, subject, content);
        }
    }

//    public static void main(String[] args) throws MessagingException {
//        String dataStr = "{\"output\":0,\"total_output\":42,\"down_time\":55,\"id\":5,\"time\":\"2025-02-15 12:52:44\",\"status\":\"DOWN\",\"uptime\":15,\"threshold\":60}";
//
//        errorNoteEmail("True" ,dataStr);
//    }
}