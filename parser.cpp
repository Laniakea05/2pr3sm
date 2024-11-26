#include <iostream> 
#include <fstream> 
#include "parser.h" 

// Функция для удаления директории
void removeDir(const fil::path& dirPath) {
    if (fil::exists(dirPath)) { // Проверяем, существует ли директория
        fil::remove_all(dirPath); // Удаляем директорию и все её содержимое
    }
}

// Функция для создания файлов и директорий на основе структуры JSON
void createfiles(const fil::path& schemePath, const json& jsonStruct, JsonTable& jatable) {
    Tables* head = nullptr; // Указатель на голову списка таблиц
    Tables* tail = nullptr; // Указатель на хвост списка таблиц

    // Проходим по всем таблицам в JSON структуре
    for (const auto& table : jsonStruct.items()) {
        fil::path tableDir = schemePath / table.key(); // Формируем путь к директории таблицы
        fil::create_directory(tableDir); // Создаем директорию для таблицы
        if (!fil::exists(tableDir)) { // Проверяем, была ли создана директория
            cout << "The directory not created " << tableDir << endl; // Если нет, выводим сообщение об ошибке
            return; 
        }

        // Создаем новый элемент списка таблиц
        Tables* newTable = new Tables{table.key(), nullptr, nullptr};

        // Добавляем новую таблицу в список
        if (head == nullptr) { // Если список пуст
            head = newTable; // Устанавливаем голову списка
            tail = newTable; // Устанавливаем хвост списка
        } else {
            tail->next = newTable; // Присоединяем новую таблицу к концу списка
            tail = newTable; // Обновляем хвост списка
        }

        // Создаем файл блокировки для таблицы
        fil::path lockDir = tableDir / (table.key() + "_lock.txt");
        ofstream file(lockDir); // Открываем файл для записи
        if (!file.is_open()) { // Проверяем, открылся ли файл
            cout << "The file not open" << endl; 
            return; 
        }
        file << "unlocked"; // Записываем состояние блокировки
        file.close(); 

        // Создаем первичный ключ
        string pkColumn = table.key() + "_pk"; // Имя колонки первичного ключа
        Node* firstColumn = new Node{pkColumn, nullptr}; // Создаем узел для первичного ключа

        Node* headColumn = firstColumn; // Указатель на голову списка колонок
        Node* tailColumn = firstColumn; // Указатель на хвост списка колонок

        // Создаем CSV файл для таблицы
        fil::path csvDir = tableDir / "1.csv"; // Путь к CSV файлу
        ofstream csvfile(csvDir); // Открываем файл для записи
        if (!csvfile.is_open()) { // Проверяем, открылся ли файл
            cout << "The file not open" << endl; 
            return; 
        }
        csvfile << pkColumn << ","; // Записываем имя колонки первичного ключа в CSV файл
        
        const auto& columns = table.value(); // Получаем колонки таблицы из JSON
        for (size_t i = 0; i < columns.size(); i++) { // Проходим по всем колонкам
            csvfile << columns[i].get<string>(); // Записываем имя колонки в CSV файл
            Node* newColumn = new Node{columns[i].get<string>(), nullptr}; // Создаем узел для колонки
            tailColumn->next = newColumn; // Присоединяем колонку к списку колонок
            tailColumn = newColumn; // Обновляем хвост списка колонок
            if (i < columns.size() - 1) { // Если это не последняя колонка
                csvfile << ","; // Добавляем запятую
            }
        }
        csvfile << endl; // Завершаем строку в CSV файле
        csvfile.close(); // Закрываем CSV файл

        newTable->column = headColumn; // Присоединяем список колонок к таблице

        // Создаем файл для хранения последовательности первичного ключа
        fil::path sequence = tableDir / (pkColumn + "_sequence.txt");
        ofstream seqfile(sequence); // Открываем файл для записи
        if (!seqfile.is_open()) { // Проверяем, открылся ли файл
            cout << "The file not open" << endl; 
            return; 
        }
        seqfile << "0"; // Записываем начальное значение последовательности
        seqfile.close(); // Закрываем файл
    }
    jatable.head = head; // Устанавливаем голову списка таблиц в JsonTable
}

// Функция для парсинга схемы из JSON файла
void parser(JsonTable& jatab) {
    ifstream file("schema.json"); // Открываем файл схемы
    if (!file.is_open()) { // Проверяем, открылся ли файл
        cout << "Not open file schema.json" << endl; 
        return; 
    }
    
    json jspars; // Объект для хранения распарсенного JSON
    file >> jspars; // Читаем содержимое файла в объект JSON
    file.close(); // Закрываем файл

    jatab.scheme = jspars["name"]; // Сохраняем имя схемы
    fil::path schemePath = fil::current_path() / jatab.scheme; // Формируем путь к директории схемы
    removeDir(schemePath); // Удаляем директорию с прошлого запуска
    fil::create_directory(schemePath); // Создаем новую директорию для схемы
    if (!fil::exists(schemePath)) { // Проверяем, была ли создана директория
        cout << "The directory not created " << schemePath << endl; // Если нет, выводим сообщение об ошибке
        return; 
    }

    if (jspars.contains("structure")) { // Проверяем, есть ли структура в JSON
        createfiles(schemePath, jspars["structure"], jatab); // Создаем файлы и директории на основе структуры
    }

    jatab.rowsCount = jspars["tuples_limit"]; // Сохраняем лимит строк из JSON
}
