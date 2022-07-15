package notification

// const (
// 	// AliTtsCodeAbleTime 预估时间不足Code
// 	AliTtsCodeAbleTime = "TTS_244145091"
// 	// AliTtsCodeAmount 转出超额Code
// 	AliTtsCodeAmount = "TTS_244145090"
// 	// AliTtsCodePenalty 节点扣罚Code
// 	AliTtsCodePenalty = "TTS_245040019"
// 	// AliTtsCodeFinish 扇区终止Code
// 	AliTtsCodeFinish = "TTS_245045016"

// 	aliTemplateID = "SMS_244615579"
// 	aliSignName   = "验证码"

// 	accessKeyID     = "abc"
// 	accessKeySecret = "acv"
// )

// // SendSMS 发短信
// func SendSMS(phone, msg string) error {
// 	err := NewAliSms(accessKeyID, accessKeySecret)(phone, msg)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// // NewAliSms 使用AK&SK初始化账号Client
// func NewAliSms(accessKeyID string, accessKeySecret string) func(phone, code string) error {
// 	return func(phone, code string) error {
// 		config := &openapi.Config{
// 			// 您的 AccessKey ID
// 			AccessKeyId: &accessKeyID,
// 			// 您的 AccessKey Secret
// 			AccessKeySecret: &accessKeySecret,
// 		}
// 		// 访问的域名
// 		config.Endpoint = tea.String("dysmsapi.aliyuncs.com")
// 		cli, err := dysmsapi20170525.NewClient(config)
// 		if err != nil {
// 			fmt.Printf(" NewClient err : %v", err.Error())
// 			return err
// 		}

// 		param := map[string]string{
// 			"code": code,
// 		}

// 		paramBytes, err := json.Marshal(param)
// 		if err != nil {
// 			fmt.Printf(" Marshal err : %v", err.Error())
// 			return err
// 		}

// 		sendSmsRequest := &dysmsapi20170525.SendSmsRequest{
// 			SignName:      tea.String(aliSignName),
// 			TemplateCode:  tea.String(aliTemplateID),
// 			PhoneNumbers:  tea.String(phone),
// 			TemplateParam: tea.String(string(paramBytes)),
// 		}
// 		runtime := &util.RuntimeOptions{}
// 		response, err := cli.SendSmsWithOptions(sendSmsRequest, runtime)
// 		if err != nil {
// 			fmt.Printf(" SendSmsWithOptions err : %v", err.Error())
// 			error := &tea.SDKError{}
// 			if _t, ok := err.(*tea.SDKError); ok {
// 				error = _t
// 			} else {
// 				error.Message = tea.String(error.Error())
// 			}
// 			// 如有需要，请打印 error
// 			fmt.Printf(" SendSmsWithOptions error : %v", error.Error())
// 			return error
// 		}

// 		fmt.Printf(" response.Body.Code : %v", response.Body.Code)

// 		if *response.Body.Code != "OK" {
// 			if *response.Body.Code == "isv.BUSINESS_LIMIT_CONTROL" {
// 				return fmt.Errorf("发送频率过快, 请稍后再试")
// 			}

// 			if *response.Body.Code == "isv.DAY_LIMIT_CONTROL" {
// 				return fmt.Errorf("已超过今日发送限制, 请稍后再试")
// 			}

// 			return fmt.Errorf("%s", *response.Body.Message)
// 		}

// 		return nil
// 	}
// }

// // SendTts 钱包预估时间不足通知
// func SendTts(phone, aliTtsCode string, param map[string]string) error {
// 	err := NewAliTts(accessKeyID, accessKeySecret)(phone, aliTtsCode, param)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// // NewAliTts 使用AK&SK初始化账号Client
// func NewAliTts(accessKeyID string, accessKeySecret string) func(phone, aliTtsCode string, param map[string]string) error {
// 	return func(phone, aliTtsCode string, param map[string]string) error {
// 		config := &openapi.Config{
// 			// 您的 AccessKey ID
// 			AccessKeyId: &accessKeyID,
// 			// 您的 AccessKey Secret
// 			AccessKeySecret: &accessKeySecret,
// 		}
// 		// 访问的域名
// 		config.Endpoint = tea.String("dyvmsapi.aliyuncs.com")
// 		client, err := dyvmsapi20170525.NewClient(config)
// 		if err != nil {
// 			fmt.Printf(" NewClient err : %v", err.Error())
// 			return err
// 		}

// 		// param := map[string]string{
// 		// 	"name":  name,
// 		// 	"value": value,
// 		// }

// 		paramBytes, err := json.Marshal(param)
// 		if err != nil {
// 			fmt.Printf(" Marshal err : %v", err.Error())
// 			return err
// 		}

// 		singleCallByTtsRequest := &dyvmsapi20170525.SingleCallByTtsRequest{
// 			// SignName:      tea.String(aliSignName),
// 			TtsCode:      tea.String(aliTtsCode),
// 			CalledNumber: tea.String(phone),
// 			TtsParam:     tea.String(string(paramBytes)),
// 		}
// 		runtime := &util.RuntimeOptions{}

// 		// 复制代码运行请自行打印 API 的返回值
// 		response, err := client.SingleCallByTtsWithOptions(singleCallByTtsRequest, runtime)
// 		if err != nil {
// 			fmt.Printf(" SingleCallByTtsWithOptions err : %v", err.Error())
// 			error := &tea.SDKError{}
// 			if _t, ok := err.(*tea.SDKError); ok {
// 				error = _t
// 			} else {
// 				error.Message = tea.String(error.Error())
// 			}
// 			// 如有需要，请打印 error
// 			fmt.Println(" SingleCallByTtsWithOptions error : ", error.Error())
// 			return error
// 		}

// 		fmt.Println(" response.Body.Code : ", response.Body.Code)

// 		if *response.Body.Code != "OK" {
// 			fmt.Println(" SingleCallByTtsWithOptions error Message : ", response.Body.Message)

// 			if *response.Body.Code == "isv.BUSINESS_LIMIT_CONTROL" {
// 				return fmt.Errorf("发送频率过快, 请稍后再试")
// 			}

// 			if *response.Body.Code == "isv.DAY_LIMIT_CONTROL" {
// 				return fmt.Errorf("已超过今日发送限制, 请稍后再试")
// 			}

// 			return fmt.Errorf("%s", *response.Body.Message)
// 		}

// 		return nil
// 	}
// }

// // VerifyPhoneFormat 验证手机号格式
// func VerifyPhoneFormat(phone string) bool {
// 	regRuler := "^1[345789]{1}\\d{9}$"
// 	reg := regexp.MustCompile(regRuler)

// 	return reg.MatchString(phone)
// }
