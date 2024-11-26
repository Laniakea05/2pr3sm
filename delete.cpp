#include <iostream>
#include "delete.h"

// Функция для удаления строк из таблицы
void deleteRows(string command, JsonTable& jatab) {
    istringstream iss(command); // Создаем поток для разбора команды
    string mes;
    iss >> mes; // Читаем первую часть команды
    iss >> mes; // Читаем вторую часть команды
    if (mes != "FROM") { // Проверяем, что вторая часть команды - "FROM"
        cout << "The data entered incorrectly." << endl; // Если нет, выводим сообщение об ошибке
        return; 
    }
    iss >> mes; // Читаем имя таблицы
    string table = mes; // Сохраняем имя таблицы
    if (!tableExist(table, jatab.head)) { // Проверяем, существует ли таблица
        cout << "Table: " << table << " not created" << endl; // Если нет, выводим сообщение об ошибке
        return; 
    }

    if (isLocked(table, jatab.scheme)) { // Проверяем, заблокирована ли таблица
        cout << "File is locked" << endl; // Если да, выводим сообщение об ошибке
        return; 
    }
    
    string command2; // Вторая команда для условия WHERE
    cout << "<< "; //ввод 
    getline(cin, command2); // Читаем всю строку
    istringstream iss2(command2); // Создаем новый поток для разбора второй команды
    string mes2;
    iss2 >> mes2; // Читаем первую часть второй команды
    if (mes2 != "WHERE") { // Проверяем, что первая часть - "WHERE"
        cout << "The data entered incorrectly." << endl; // Если нет, выводим сообщение об ошибке
        return; 
    }
    iss2 >> mes2; // Читаем условия
    string table2; // Вторая таблица
    string column; // Имя колонки
    separateDot(mes2, table2, column); // Разделяем таблицу и колонку
    if (table2 != table) { // Проверяем, совпадает ли таблица
        cout << "The data entered incorrectly." << endl; // Если нет, выводим сообщение об ошибке
        return; 
    }
    if (!columnExist(table, column, jatab.head)) { // Проверяем, существует ли колонка
        cout << "This column does not exist" << endl; // Если нет, выводим сообщение об ошибке
        return; 
    }
    iss2 >> mes2; // Читаем оператор сравнения
    if (mes2 != "=") { // Проверяем, что оператор - "="
        cout << "The data entered incorrectly." << endl; // Если нет, выводим сообщение об ошибке
        return; 
    }
    iss2 >> mes2; // Читаем значение для сравнения
    if (mes2.front() != '\'' || mes2.back() != '\'') { // Проверяем, заключено ли значение в кавычки
        cout << "The data entered incorrectly." << endl; // Если нет, выводим сообщение об ошибке
        return; 
    }
    string value; // Значение для сравнения
    for (size_t i=1; i < mes2.size() - 1; i++) { // Извлекаем значение из кавычек
        value += mes2[i];
    }

    locker(table, jatab.scheme); // Блокируем таблицу для изменения

    size_t csvCount = 1; // Счетчик для CSV файлов
    while (true) { // Цикл для нахождения количества CSV файлов
        fil::path csvPath = fil::current_path() / jatab.scheme / table / (to_string(csvCount) + ".csv"); // Формируем путь к файлу
        ifstream file(csvPath); // Открываем файл
        if (!file.is_open()) { // Если файл не открыт
            break; // Выходим из цикла
        }
        file.close(); // Закрываем файл
        csvCount++; // Увеличиваем счетчик
    }
    
    for (size_t i = 1; i < csvCount; i++) { // Цикл для обработки всех CSV файлов
        fil::path csvPath = fil::current_path() / jatab.scheme / table / (to_string(i) + ".csv"); // Формируем путь к файлу
        rapidcsv::Document csvDoc(csvPath.string()); // Загружаем документ CSV
        int index = csvDoc.GetColumnIdx(column); // Получаем индекс колонки
        size_t countRows = csvDoc.GetRowCount(); // Получаем количество строк
        for (size_t j = 0; j < countRows; j++) { // Цикл для удаления строк
            if (csvDoc.GetCell<string>(index, j) == value) { // Если значение в ячейке совпадает
                csvDoc.RemoveRow(j); // Удаляем строку
                countRows--; // Уменьшаем количество строк
                j--; // Уменьшаем индекс, чтобы не пропустить следующую строку
            }
        }
        csvDoc.Save(csvPath.string()); // Сохраняем изменения в CSV файл
    }
    
    locker(table, jatab.scheme); // Разблокируем таблицу
}
