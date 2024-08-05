from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError



class GrowthSlackBot:
    def __init__(self, token):
        self.client = WebClient(token=token)

    def send_message(self, 
                     user_id: str, 
                     text: str
                     ):
        
        message = {
            "channel": user_id,
            "text": text
        }

        try:
            response = self.client.chat_postMessage(**message)
            print("메시지가 성공적으로 전송되었습니다.")
        except SlackApiError as e:
            print(f"메시지 전송 실패: {e.response['error']}")


    def send_message_with_files(self, 
                     channel_name: str, 
                     message: str,
                     files: list,
                     ):
        try:

            for file in files:
                upload = self.client.files_upload(file=file, filename=file)
                message = message + "<" + upload["file"]["permalink"] + "| >"

            response = self.client.chat_postMessage(
                channel=channel_name, 
                text=message
                )
            
            print("엑셀 파일이 성공적으로 전송되었습니다.")
        except SlackApiError as e:
            print(f"엑셀 파일 전송 실패: {e.response['error']}")