package notification

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/smtp"
	"regexp"
	"text/template"
	"time"

	"titan-ultra-network/errorcode"
	"titan-ultra-network/log"
	"titan-ultra-network/redishelper"

	"github.com/go-gomail/gomail"
)

var (
	emailChan = make(chan EmailBody, 10000)
	mUser     = ""
	mPwd      = ""
	mHost     = ""
	mPort     = 0
	send      = false

	codMsg = "您的验证码是%s。您正在使用随机密码登录服务,如非本人操作,请尽快修改密码。"
)

// EmailBody 邮件验证码body
type EmailBody struct {
	Emails []string `json:"emails"`
	// Code     string   `json:"code"`
	PlayerID int    `json:"playerID"`
	Subject  string `json:"subject"` // 标题
	Content  string `json:"content"` // 内容
}

// InitEmailPost 初始化邮件post
func InitEmailPost(user, pwd, host string, port int, isSend bool) {
	mUser = user
	mPwd = pwd
	mHost = host
	mPort = port
	send = isSend

	// 用chan发送邮件 不阻塞主线程
	for msg := range emailChan {
		// sendToOutLook(msg)
		sendEmail(msg)
	}
}

// VerifyEmailFormat 验证邮箱
func VerifyEmailFormat(email string) bool {
	pattern := `^[0-9a-z][_.0-9a-z-]{0,31}@([0-9a-z][0-9a-z-]{0,30}[0-9a-z]\.){1,4}[a-z]{2,4}$`
	reg := regexp.MustCompile(pattern)
	return reg.MatchString(email)
}

// AddEmailOfCode 新增验证邮件
func AddEmailOfCode(email, subject string, cid int) error {
	// otpCode 生成的六位数验证码
	otpCode := newCode()

	body := EmailBody{
		// Code:     otpCode,
		Emails:   []string{email},
		PlayerID: cid,
		Subject:  subject,
		Content:  fmt.Sprintf(codMsg, otpCode),
	}

	// 测试的时候 不发送
	if send {
		emailChan <- body
	}

	// log.Infof("---------SendMail body:%v", body)

	otpAccount := redishelper.OtpAccount{
		Account: email, // req.GetPlayerID(),
	}

	err := otpAccount.Add(otpCode)
	if err != nil {
		log.Errorf("SendMail failed add otp:%v to redis, email:%v err:%v", otpCode, email, err.Error())
		return err
	}

	return nil
}

// AddEmailOfBase 新增普通邮件
func AddEmailOfBase(emails []string, cid int, msg, subject string) error {
	body := EmailBody{
		Emails:   emails,
		PlayerID: cid,
		Subject:  subject,
		Content:  msg,
	}

	// 测试的时候 不发送
	if send {
		emailChan <- body
	}

	log.Infof("---------SendMail body:%v", body)
	return nil
}

// newCode 生成6位数随机验证码 string类型
func newCode() string {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	otpCode := fmt.Sprintf("%6v", rnd.Int31n(900000)+100000)
	return otpCode
}

// CheckCode 检查验证码
func CheckCode(email, code string) errorcode.ErrorCode {
	otpAccount := redishelper.OtpAccount{
		Account: email,
	}

	otpCode, err := otpAccount.Get()
	if err != nil {
		log.Errorf("CheckCode failed get email:%v code err:%v\n", email, err.Error())
		return errorcode.RedisError
	}

	if code != otpCode {
		log.Error("CheckCode failed code != otpCode : ", otpCode)
		return errorcode.CodeMismatch
	}

	// 验证完成 删除验证码
	otpAccount.Del()

	return errorcode.Success
}

// GetCode 获取验证码
func GetCode(email string) (string, error) {
	otpAccount := redishelper.OtpAccount{
		Account: email,
	}

	otpCode, err := otpAccount.Get()
	if err != nil {
		log.Errorf("CheckCode failed get email:%v code err:%v\n", email, err.Error())
		return "", err
	}

	return otpCode, nil
}

func sendToOutLook(req EmailBody) error {
	subject := req.Subject
	content1 := req.Content
	// content2 := req.Code
	tos := req.Emails
	// Sender data.
	from := mUser
	password := mPwd

	// smtp server configuration.
	smtpHost := mHost
	smtpPort := fmt.Sprintf("%d", mPort)
	addr := fmt.Sprintf("%s:%s", smtpHost, smtpPort)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Errorln("dial error:", err)
		return err
	}

	c, err := smtp.NewClient(conn, smtpHost)
	if err != nil {
		log.Errorln("NewClient error:", err)
		return err
	}

	tlsconfig := &tls.Config{
		ServerName: smtpHost,
	}

	if err = c.StartTLS(tlsconfig); err != nil {
		log.Errorln("StartTLS error:", err)
		return err
	}

	auth := LoginAuth(from, password)

	if err = c.Auth(auth); err != nil {
		log.Errorf("Auth error:%v", err)
		return err
	}

	t, _ := template.ParseFiles("template.html")

	var body bytes.Buffer

	mimeHeaders := "MIME-version: 1.0;\nContent-Type: text/html; charset=\"UTF-8\";\n\n"
	body.Write([]byte(fmt.Sprintf("Subject: %s \n%s\n\n", subject, mimeHeaders)))

	t.Execute(&body, struct {
		Name string
		Code string
	}{
		Name: content1,
		// Code: content2,
	})

	// Sending email.
	err = smtp.SendMail(smtpHost+":"+smtpPort, auth, from, tos, body.Bytes())
	if err != nil {
		log.Errorln("SendMail error:", err)
		return err
	}

	log.Infoln("Email Sent!")
	return nil
}

type loginAuth struct {
	username, password string
}

// LoginAuth 登录
func LoginAuth(username, password string) smtp.Auth {
	return &loginAuth{username, password}
}

func (a *loginAuth) Start(server *smtp.ServerInfo) (string, []byte, error) {
	return "LOGIN", []byte(a.username), nil
}

func (a *loginAuth) Next(fromServer []byte, more bool) ([]byte, error) {
	if more {
		switch string(fromServer) {
		case "Username:":
			return []byte(a.username), nil
		case "Password:":
			return []byte(a.password), nil
		default:
			return nil, errors.New("Unknown from server")
		}
	}
	return nil, nil
}

// sendEmail 企业邮箱
func sendEmail(eBody EmailBody) error {
	// subject := "通知"
	// body := `您的worker钱包预估时间已不足<h3>10</h3>分钟，请充值`

	m := gomail.NewMessage()
	m.SetHeader("To", eBody.Emails...)
	m.SetAddressHeader("From", mUser, "")

	// toers []string
	// m.SetHeader("To", toers...)
	// 主题
	m.SetHeader("Subject", eBody.Subject)

	// 正文
	m.SetBody("text/html", eBody.Content)

	d := gomail.NewPlainDialer(mHost, mPort, mUser, mPwd)
	// 发送
	return d.DialAndSend(m)
}
