import os

# 대상 폴더 경로 설정
folder_path = '/home/aiot/rpi4_kafka_test/logging'

# 폴더 내 파일 모두 삭제
for filename in os.listdir(folder_path):
    file_path = os.path.join(folder_path, filename)
    try:
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)  # 파일 삭제
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)  # 서브폴더 삭제 (필요하면 추가)
    except Exception as e:
        print(f"Error: {file_path} 삭제 실패 - {e}")

print(f"{folder_path} 내 모든 파일이 삭제되었습니다.")