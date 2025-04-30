import requests

def main():
    print("Hello There")
    try:
        url = "https://www.eicar.org/download/eicar.com"
        response = requests.get(url)
        if response.status_code == 200:
            with open("eicar.com", "wb") as file:
                file.write(response.content)
            print("Download successful.")
        else:
            print("Failed to download the file. Status code:", response.status_code)
    except Exception as e:
        print("An error occurred:", e)

if __name__ == "__main__":
    main()
