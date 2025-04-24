from kafka import KafkaProducer
import time
import json
import sys


def main():
    server = input("서버주소 기입 : ")
    topic = input("topic명 기입 : ")

    try:
        producer = KafkaProducer(bootstrap_servers=server)
    except Exception as e:
        print(f"Kafka 연결 실패: {e}")
        sys.exit(1)

    print("\n✏️ 메시지를 입력하세요. 종료하려면 'exit'을 입력하세요.\n")
    while True:
        try:
            msg = input("You : ")
            if msg.lower() == "exit":
                break
            future = producer.send(topic, msg.encode("utf-8"))
            result = future.get(timeout=10)  # 결과 기다리기
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"전송 중 오류 발생: {e}")

    print("exit!")



