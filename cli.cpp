#include <netinet/in.h> // Подключение заголовочного файла для работы с сетевыми протоколами
#include <iostream>     
#include <string>      
#include <unistd.h>    // Подключение заголовочного файла для работы с POSIX API (например, для close)

// Используем пространство имен std для упрощения записи
using namespace std;

int main() {
    // Создание сокета для клиента (IPv4, TCP)
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
    
    // Определение структуры для адреса сервера
    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET; // Указываем семью адресов (IPv4)
    serverAddr.sin_port = htons(7432); // Указываем порт, к которому будем подключаться (7432)
    serverAddr.sin_addr.s_addr = INADDR_ANY; // Указываем, что принимаем соединения на любом доступном интерфейсе

    // Подключение к серверу
    connect(clientSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr));

    string command; // Переменная для хранения введенной команды
    while (true) {
        cout << "<< "; // Вывод приглашения для ввода команды
        getline(cin, command); // Чтение строки из стандартного ввода

        if (command.empty()) continue; // Если команда пустая, продолжаем цикл

        // Отправка команды на сервер
        send(clientSocket, command.c_str(), command.size(), 0);

        // Если введена команда "EXIT", выходим из цикла
        if (command == "EXIT") break;

        char buffer[1024]; // Буфер для получения ответа от сервера
        int bytesReceived = recv(clientSocket, buffer, sizeof(buffer), 0); // Получение данных от сервера
        if (bytesReceived > 0) { // Если данные были получены
            cout << "Server: " << string(buffer, bytesReceived) << endl; // Вывод ответа сервера
        }
    }
    
    close(clientSocket); // Закрытие сокета клиента
    return 0; // Завершение программы
}