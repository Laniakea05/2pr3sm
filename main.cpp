#include <iostream>     // Подключение заголовочного файла для работы с вводом/выводом
#include <string>       // Подключение заголовочного файла для работы со строками
#include <thread>       // Подключение заголовочного файла для работы с потоками
#include <mutex>        // Подключение заголовочного файла для работы с мьютексами
#include <unistd.h>     // Подключение заголовочного файла для работы с POSIX API (например, для close)
#include <sstream>      // Подключение заголовочного файла для работы со строковыми потоками
#include "insert.h"     // Подключение заголовочного файла для работы с функцией вставки
#include "delete.h"     // Подключение заголовочного файла для работы с функцией удаления
#include "parser.h"     // Подключение заголовочного файла для работы с парсером
#include "select.h"     // Подключение заголовочного файла для работы с функцией выбора
#include <netinet/in.h> // Подключение заголовочного файла для работы с сетевыми протоколами
#include <arpa/inet.h>  // Подключение заголовочного файла для работы с сетевыми адресами

using namespace std;

mutex dbMutex; // Мьютекс для синхронизации доступа к таблице
mutex clientMutex; // Мьютекс для синхронизации доступа к счетчику клиентов
int activeClients = 0; // Счетчик активных клиентов

// Функция для обработки клиента
void handleClient(int clientSocket, JsonTable& jatab) {
    char buffer[1024]; // Буфер для получения данных от клиента
    while (true) {
        int bytesReceived = recv(clientSocket, buffer, sizeof(buffer), 0); // Получение данных от клиента
        if (bytesReceived <= 0) break; // Прерываем цикл, если клиент отключился или произошла ошибка

        string command(buffer, bytesReceived); // Преобразование полученных данных в строку
        istringstream iss(command); // Создание строкового потока для разбора команды
        string firstmes; // Переменная для хранения первой части команды
        iss >> firstmes; // Извлечение первой части команды

        // Блокировка на время выполнения команды
        dbMutex.lock(); // Захват мьютекса для синхронизации доступа к базе данных
        if (firstmes == "INSERT") {
            cout << "the insert command has been entered." << endl; // Сообщение о выполнении команды вставки
            insert(command, jatab); // Вызов функции вставки
        } else if (firstmes == "DELETE") {
            cout << "the delete command has been entered." << endl; // Сообщение о выполнении команды удаления
            deleteRows(command, jatab); // Вызов функции удаления
        } else if (firstmes == "SELECT") {
            cout << "the selection command has been entered." << endl; // Сообщение о выполнении команды выбора
            select(command, jatab); // Вызов функции выбора
        } else if (firstmes == "EXIT") {
            dbMutex.unlock(); // Освобождение мьютекса перед выходом
            break; // Выход из цикла, если команда "EXIT"
        }
        dbMutex.unlock(); // Освобождение мьютекса

        // Отправляем подтверждение выполнения команды клиенту
        send(clientSocket, "The command has been sent\n", 38, 0);
    }

    cout << "Client " << activeClients << " disconnected."  << endl; // Сообщение о отключении клиента

    // Закрытие сокета клиента
    close(clientSocket);

    // Уменьшаем количество активных клиентов после завершения работы с клиентом
    lock_guard<mutex> lock(clientMutex); // Защита доступа к счетчику клиентов
    activeClients--; // Уменьшение счетчика активных клиентов

    // Если больше нет активных клиентов, завершаем сервер
    if (activeClients == 0) {
        cout << "All clients are disconnected. The server is shutting down\n";
        exit(0); // Завершение работы сервера
    }
}

int main() {
    JsonTable jatab; // Создание объекта таблицы
    parser(jatab); // Инициализация таблицы при запуске

    // Создание сокета для сервера
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in serverAddr; // Структура для хранения адреса сервера
    serverAddr.sin_family = AF_INET; // Указываем семью адресов (IPv4)
    serverAddr.sin_port = htons(7432); // Указываем порт, на котором будет слушать сервер (7432)
    serverAddr.sin_addr.s_addr = INADDR_ANY; // Указываем, что принимаем соединения на любом доступном интерфейсе

    bind(serverSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)); // Привязка сокета к адресу
    listen(serverSocket, 3); // Начало прослушивания входящих соединений

    cout << "Сервер запущен и слушает порт 7432...\n"; // Сообщение о запуске сервера

    while (true) {
        sockaddr_in clientAddr; // Структура для хранения адреса клиента
        socklen_t clientLen = sizeof(clientAddr); // Длина адреса клиента
        int clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddr, &clientLen); // Принятие входящего соединения

        if (clientSocket < 0) {
            cerr << "Ошибка подключения клиента\n"; // Сообщение об ошибке подключения
            continue; // Продолжаем цикл, если возникла ошибка
        }

        string clientIp = inet_ntoa(clientAddr.sin_addr); // Получение IP-адреса клиента
        cout << "Клиент " << (activeClients + 1) << " подключился: " << clientIp << endl; // Сообщение о подключении клиента

        // Увеличиваем количество активных клиентов в главном потоке
        lock_guard<mutex> lock(clientMutex); // Защита доступа к счетчику клиентов
        activeClients++; // Увеличение счетчика активных клиентов

        // Запуск потока для обработки запроса клиента
        thread clientThread(handleClient, clientSocket, ref(jatab)); // Создание потока для обработки клиента
        clientThread.detach(); // Отделение потока, чтобы он работал независимо
    }

    close(serverSocket); // Закрытие серверного сокета
    return 0; // Завершение программы
}
