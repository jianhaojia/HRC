package org.HFC.SQM.utils;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class QQEmailSender {

    /**
     * 使用 QQ 邮箱发送邮件的方法
     * @param username 发件人 QQ 邮箱用户名（完整邮箱地址）
     * @param authCode 发件人 QQ 邮箱的 SMTP 授权码
     * @param to 收件人邮箱地址
     * @param subject 邮件主题
     * @param content 邮件内容
     * @throws MessagingException 处理邮件发送过程中可能出现的异常
     */
    public static void sendQQEmail(String username, String authCode, String to, String subject, String content) throws MessagingException {
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
        message.setSubject(subject);
        message.setText(content);

        // 发送邮件
        Transport.send(message);
    }
    public static void errorNoteEmail(String flag,String info) throws MessagingException {
        if(flag=="True") {
            System.out.println("停机时间大于1分钟");
        }else {   //            // 发件人 QQ 邮箱地址
            String username = "9***@qq.com";
            // 发件人 QQ 邮箱的 SMTP 授权码
            String authCode = "*******";
            // 收件人邮箱地址
            String to = "9**@qq.com";
            // 邮件主题
            String subject = "停机警告";
            // 邮件内容
            String content ="" ;

            sendQQEmail(username, authCode, to, subject, content);
            }
    }



}