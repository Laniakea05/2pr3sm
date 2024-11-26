#include "tableWork.h" // Подключаем заголовочный файл для работы с таблицами

// Функция для проверки, заблокирована ли таблица
bool isLocked(string table, string scheme) {
    // Формируем путь к файлу блокировки
    fil::path filePath = fil::current_path() / scheme / table / (table + "_lock.txt");
    ifstream file(filePath); // Открываем файл
    // Проверяем, удалось ли открыть файл
    if (!file.is_open()) {
        cout << "The file not open." << endl; // Сообщаем об ошибке
        return true; // Возвращаем true, так как таблица считается заблокированной
    }
    string lock;
    file >> lock; // Читаем состояние блокировки
    file.close(); // Закрываем файл
    // Если состояние "locked", возвращаем true
    if (lock == "locked") return true;
    return false; // В противном случае возвращаем false
}

// Функция для переключения состояния блокировки таблицы
void locker(string table, string scheme) {
    // Формируем путь к файлу блокировки
    fil::path filePath = fil::current_path() / scheme / table / (table + "_lock.txt");
    ifstream file(filePath); // Открываем файл
    // Проверяем, удалось ли открыть файл
    if (!file.is_open()) {
        cout << "The file not open." << endl; // Сообщаем об ошибке
        return; // Завершаем выполнение функции
    }
    string lock;
    file >> lock; // Читаем состояние блокировки
    file.close(); // Закрываем файл
    ofstream fileOut(filePath); // Открываем файл для записи
    // Проверяем, удалось ли открыть файл для записи
    if (!fileOut.is_open()) {
        cout << "The file not open." << endl; // Сообщаем об ошибке
        return; // Завершаем выполнение функции
    }
    // Переключаем состояние блокировки
    if (lock == "locked") {
        fileOut << "unlocked"; // Если заблокирована, разблокируем
    }
    else {
        fileOut << "locked"; // Если не заблокирована, блокируем
    }
    fileOut.close(); // Закрываем файл
}

// Функция для проверки существования таблицы
bool tableExist(string table, Tables* head) {
    Tables* current = head; // Начинаем с головы списка таблиц
    // Проходим по списку таблиц
    while (current != nullptr) {
        if (current->name == table) { // Если таблица найдена
            return true; // Возвращаем true
        }
        current = current->next; // Переходим к следующей таблице
    }
    return false; // Если таблица не найдена, возвращаем false
}

// Функция для проверки существования колонки в таблице
bool columnExist(string table, string columnName, Tables* head) {
    Tables* current = head; // Начинаем с головы списка таблиц
    // Проходим по списку таблиц
    while (current != nullptr) {
        if (current->name == table) { // Если таблица найдена
            Node* currentColumn = current->column; // Начинаем с головы списка колонок
            // Проходим по списку колонок
            while (currentColumn != nullptr) {
                if (currentColumn->data == columnName) { // Если колонка найдена
                    return true; // Возвращаем true
                }
                currentColumn = currentColumn->next; // Переходим к следующей колонке
            }
        }
        current = current->next; // Переходим к следующей таблице
    }
    return false; // Если колонка не найдена, возвращаем false
}

// Функция для разделения строки на имя таблицы и имя колонки
void separateDot(string mes, string& table, string& column) {
    bool isDot = false; // Флаг для проверки наличия точки
    // Проходим по строке
    for (size_t i=0; i < mes.size(); i++) {
        if (mes[i] == '.') { // Если встречаем точку
            isDot = true; // Устанавливаем флаг
            i++; // Переходим к следующему символу
        }
        if (mes[i] == ',') { // Игнорируем запятые
            continue;
        }
        // Если точка не встречалась, добавляем символ к имени таблицы
        if (!isDot) table += mes[i];
        else column += mes[i]; // Иначе добавляем к имени колонки
    }
    // Если точка не была найдена, выводим сообщение об ошибке
    if (!isDot) {
        cout << "The data entered is incorrect." << endl;
        return;
    }
}

// Функция для выполнения кросс-джоина двух таблиц
void crossJoin(string table1, string column1, string table2, string column2, JsonTable& jatab) {
    size_t csvCount1 = 1; // Счетчик для первой таблицы
    // Ищем количество CSV файлов для первой таблицы
    while (true) {
        fil::path csvPath1 = fil::current_path() / jatab.scheme / table1 / (to_string(csvCount1) + ".csv");
        ifstream file1(csvPath1); // Открываем файл
        if (!file1.is_open()) { // Если файл не открыт, выходим из цикла
            break;
        }
        file1.close(); // Закрываем файл
        csvCount1++; // Увеличиваем счетчик
    }
    size_t csvCount2 = 1; // Счетчик для второй таблицы
    // Ищем количество CSV файлов для второй таблицы
    while (true) {
        fil::path csvPath2 = fil::current_path() / jatab.scheme / table2 / (to_string(csvCount2) + ".csv");
        ifstream file2(csvPath2); // Открываем файл
        if (!file2.is_open()) { // Если файл не открыт, выходим из цикла
            break;
        }
        file2.close(); // Закрываем файл
        csvCount2++; // Увеличиваем счетчик
    }
    // Вывод заголовков для кросс-джоина
    cout << "|" << setw(10) << left << (table1 + "_pk") << " |" << setw(10) << left << column1 << " |";
    cout << setw(10) << left << (table2 + "_pk") << " |" << setw(10) << left << column2 << " |" << endl;

    // Проходим по всем строкам первой таблицы
    for (size_t csvi1 = 1; csvi1 < csvCount1; csvi1++) {
        fil::path csvPath1 = fil::current_path() / jatab.scheme / table1 / (to_string(csvi1) + ".csv");
        rapidcsv::Document csvDoc1(csvPath1.string()); // Загружаем CSV файл
        int idxColumn1 = csvDoc1.GetColumnIdx(column1); // Получаем индекс колонки
        size_t rowsCount1 = csvDoc1.GetRowCount(); // Получаем количество строк

        // Проходим по всем строкам второй таблицы
        for (size_t i=0; i < rowsCount1; i++) {
            for (size_t csvi2 = 1; csvi2 < csvCount2; csvi2++) {
                fil::path csvPath2 = fil::current_path() / jatab.scheme / table2 / (to_string(csvi2) + ".csv");
                rapidcsv::Document csvDoc2(csvPath2.string()); // Загружаем CSV файл
                int idxColumn2 = csvDoc2.GetColumnIdx(column2); // Получаем индекс колонки
                size_t rowsCount2 = csvDoc2.GetRowCount(); // Получаем количество строк

                // Выводим данные из первой и второй таблиц
                for (size_t j=0; j < rowsCount2; j++) {
                    cout << "|" << setw(10) << left << csvDoc1.GetCell<string>(0, i) << " |" << setw(10) << left << csvDoc1.GetCell<string>(idxColumn1, i) << " |";
                    cout << setw(10) << left << csvDoc2.GetCell<string>(0, j) << " |" << setw(10) << left << csvDoc2.GetCell<string>(idxColumn2, j) << " |" << endl;
                }
            }
        }
    }
}

// Функция для выполнения зависимого кросс-джоина двух таблиц
void crossJoinDepend(string table1, string column1, string table2, string column2, JsonTable& jatab) {
    size_t csvCount1 = 1; // Счетчик для первой таблицы
    // Ищем количество CSV файлов для первой таблицы
    while (true) {
        fil::path csvPath1 = fil::current_path() / jatab.scheme / table1 / ("res_" + to_string(csvCount1) + ".csv");
        ifstream file1(csvPath1); // Открываем файл
        if (!file1.is_open()) { // Если файл не открыт, выходим из цикла
            break;
        }
        file1.close(); // Закрываем файл
        csvCount1++; // Увеличиваем счетчик
    }
    size_t csvCount2 = 1; // Счетчик для второй таблицы
    // Ищем количество CSV файлов для второй таблицы
    while (true) {
        fil::path csvPath2 = fil::current_path() / jatab.scheme / table2 / ("res_" + to_string(csvCount2) + ".csv");
        ifstream file2(csvPath2); // Открываем файл
        if (!file2.is_open()) { // Если файл не открыт, выходим из цикла
            break;
        }
        file2.close(); // Закрываем файл
        csvCount2++; // Увеличиваем счетчик
    }
    // Вывод заголовков для зависимого кросс-джоина
    cout << "|" << setw(10) << left << (table1 + "_pk") << " |" << setw(10) << left << column1 << " |";
    cout << setw(10) << left << (table2 + "_pk") << " |" << setw(10) << left << column2 << " |" << endl;

    // Проходим по всем строкам первой таблицы
    for (size_t csvi1 = 1; csvi1 < csvCount1; csvi1++) {
        fil::path csvPath1 = fil::current_path() / jatab.scheme / table1 / ("res_" + to_string(csvi1) + ".csv");
        rapidcsv::Document csvDoc1(csvPath1.string()); // Загружаем CSV файл
        int idxColumn1 = csvDoc1.GetColumnIdx(column1); // Получаем индекс колонки
        size_t rowsCount1 = csvDoc1.GetRowCount(); // Получаем количество строк

        // Проходим по всем строкам второй таблицы
        for (size_t i=0; i < rowsCount1; i++) {
            for (size_t csvi2 = 1; csvi2 < csvCount2; csvi2++) {
                fil::path csvPath2 = fil::current_path() / jatab.scheme / table2 / ("res_" + to_string(csvi2) + ".csv");
                rapidcsv::Document csvDoc2(csvPath2.string()); // Загружаем CSV файл
                int idxColumn2 = csvDoc2.GetColumnIdx(column2); // Получаем индекс колонки
                size_t rowsCount2 = csvDoc2.GetRowCount(); // Получаем количество строк

                // Выводим данные из первой и второй таблиц
                for (size_t j=0; j < rowsCount2; j++) {
                    cout << "|" << setw(10) << left << csvDoc1.GetCell<string>(0, i) << " |" << setw(10) << left << csvDoc1.GetCell<string>(idxColumn1, i) << " |";
                    cout << setw(10) << left << csvDoc2.GetCell<string>(0, j) << " |" << setw(10) << left << csvDoc2.GetCell<string>(idxColumn2, j) << " |" << endl;
                }
            }
        }
    }
}

// Функция для обработки условия с одной таблицей
void condSingleT(JsonTable& jatab, string tableCond1, string tableCond2, string columnCond1, string columnCond2) {
    size_t i = 0; // Индекс для итерации
    while (true) {
        size_t icsv = i/jatab.rowsCount + 1; // Номер CSV файла
        size_t row = i%jatab.rowsCount; // Номер строки
        fil::path csvPathCond1 = fil::current_path() / jatab.scheme / tableCond1 / (to_string(icsv) + ".csv");
        fil::path csvPathCond2 = fil::current_path() / jatab.scheme / tableCond2 / (to_string(icsv) + ".csv");
        ifstream fileCond1(csvPathCond1); // Открываем файл первой таблицы
        ifstream fileCond2(csvPathCond2); // Открываем файл второй таблицы
        // Если хотя бы один из файлов не открыт, выходим из цикла
        if (!fileCond1.is_open() || !fileCond2.is_open()) {
            break;
        }
        fileCond1.close(); // Закрываем файл первой таблицы
        fileCond2.close(); // Закрываем файл второй таблицы

        // Формируем пути к результатам
        fil::path csvPathRes1 = fil::current_path() / jatab.scheme / tableCond1 / ("res_" + to_string(icsv) + ".csv");
        fil::path csvPathRes2 = fil::current_path() / jatab.scheme / tableCond2 / ("res_" + to_string(icsv) + ".csv");
        ifstream fileRes1(csvPathRes1); // Открываем файл результата первой таблицы
        // Если файл результата не открыт, создаем его
        if (!fileRes1.is_open()) {
            ofstream fileRes1Out(csvPathRes1);
            if (!fileRes1Out.is_open()) {
                cout << "Not open file: " << csvPathRes1 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            fileRes1Out.close(); // Закрываем файл
        }
        else {
            fileRes1.close(); // Закрываем файл
        }

        ifstream fileRes2(csvPathRes2); // Открываем файл результата второй таблицы
        // Если файл результата не открыт, создаем его
        if (!fileRes2.is_open()) {
            ofstream fileRes2Out(csvPathRes2);
            if (!fileRes2Out.is_open()) {
                cout << "Not open file: " << csvPathRes2 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            fileRes2Out.close(); // Закрываем файл
        }
        else {
            fileRes2.close(); // Закрываем файл
        }

        // Формируем пути к первому и конечному CSV файлам
        fil::path csvFirstPath1 = fil::current_path() / jatab.scheme / tableCond1 / "1.csv";
        fil::path csvEndPath1 = fil::current_path() / jatab.scheme / tableCond1 / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc1(csvEndPath1.string()); // Загружаем конечный CSV файл
        // Если файл пустой, копируем колонки из первого файла
        if (doc1.GetRowCount() == 0) {
            cpColumns(csvFirstPath1, csvEndPath1);
        }
        fil::path csvFirstPath2 = fil::current_path() / jatab.scheme / tableCond2 / "1.csv";
        fil::path csvEndPath2 = fil::current_path() / jatab.scheme / tableCond2 / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc2(csvEndPath2.string()); // Загружаем конечный CSV файл второй таблицы
        // Если файл пустой, копируем колонки из первого файла
        if (doc2.GetRowCount() == 0) {
            cpColumns(csvFirstPath2, csvEndPath2);
        }
        // Проверяем зависимость таблиц
        if (isDependenceTables(tableCond1, columnCond1, tableCond2, columnCond2, icsv, row, jatab)) {
            rapidcsv::Document docCond1(csvPathCond1.string()); // Загружаем CSV файл первой таблицы
            size_t columnsCount1 = docCond1.GetColumnCount(); // Получаем количество колонок
            ofstream csvResFile1(csvPathRes1, ios::app); // Открываем файл результата первой таблицы для добавления
            // Проверяем, удалось ли открыть файл
            if (!csvResFile1.is_open()) {
                cout << "Not open file: " << csvPathRes1 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            // Записываем данные в файл результата первой таблицы
            for (size_t j=0; j < columnsCount1; j++) {
                if (j+1 != columnsCount1) {
                    string currentCell = docCond1.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile1 << currentCell << ","; // Записываем значение с запятой
                }
                else {
                    string currentCell = docCond1.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile1 << currentCell; // Записываем последнее значение без запятой
                }
            }
            csvResFile1 << endl; // Переход на новую строку
            csvResFile1.close(); // Закрываем файл

            rapidcsv::Document docCond2(csvPathCond2.string()); // Загружаем CSV файл второй таблицы
            size_t columnsCount2 = docCond2.GetColumnCount(); // Получаем количество колонок
            ofstream csvResFile2(csvPathRes2, ios::app); // Открываем файл результата второй таблицы для добавления
            // Проверяем, удалось ли открыть файл
            if (!csvResFile2.is_open()) {
                cout << "Not open file: " << csvPathRes2 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            // Записываем данные в файл результата второй таблицы
            for (size_t j=0; j < columnsCount2; j++) {
                if (j+1 != columnsCount2) {
                    string currentCell = docCond2.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile2 << currentCell << ","; // Записываем значение с запятой
                }
                else {
                    string currentCell = docCond2.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile2 << currentCell; // Записываем последнее значение без запятой
                }
            }
            csvResFile2 << endl; // Переход на новую строку
            csvResFile2.close(); // Закрываем файл
        }
        i++; // Увеличиваем индекс
    }
}

// Функция для обработки условия с одной строкой
void condSingleS(JsonTable& jatab, string tableCond1, string table, string columnCond1, string sCond) {
    size_t i = 0; // Инициализируем индекс для итерации
    while (true) {
        // Вычисляем номер CSV файла и номер строки
        size_t icsv = i / jatab.rowsCount + 1;
        size_t row = i % jatab.rowsCount;

        // Формируем путь к CSV файлу первой таблицы
        fil::path csvPathCond1 = fil::current_path() / jatab.scheme / tableCond1 / (to_string(icsv) + ".csv");
        ifstream fileCond1(csvPathCond1); // Открываем файл
        if (!fileCond1.is_open()) { // Если файл не открыт, выходим из цикла
            break;
        }
        fileCond1.close(); // Закрываем файл

        // Формируем пути к файлам результатов
        fil::path csvPathRes1 = fil::current_path() / jatab.scheme / tableCond1 / ("res_" + to_string(icsv) + ".csv");
        fil::path csvPathRes2 = fil::current_path() / jatab.scheme / table / ("res_" + to_string(icsv) + ".csv");

        // Проверяем, существует ли файл результата для первой таблицы
        ifstream fileRes1(csvPathRes1);
        if (!fileRes1.is_open()) { // Если файл не открыт
            // Создаем файл результата
            ofstream fileRes1Out(csvPathRes1);
            if (!fileRes1Out.is_open()) { // Если не удалось открыть файл
                cout << "Not open file: " << csvPathRes1 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            fileRes1Out.close(); // Закрываем файл
        } else {
            fileRes1.close(); // Закрываем файл
        }

        // Проверяем, существует ли файл результата для второй таблицы
        ifstream fileRes2(csvPathRes2);
        if (!fileRes2.is_open()) { // Если файл не открыт
            // Создаем файл результата
            ofstream fileRes2Out(csvPathRes2);
            if (!fileRes2Out.is_open()) { // Если не удалось открыть файл
                cout << "Not open file: " << csvPathRes2 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            fileRes2Out.close(); // Закрываем файл
        } else {
            fileRes2.close(); // Закрываем файл
        }

        // Формируем пути к первому и конечному CSV файлам первой таблицы
        fil::path csvFirstPath1 = fil::current_path() / jatab.scheme / tableCond1 / "1.csv";
        fil::path csvEndPath1 = fil::current_path() / jatab.scheme / tableCond1 / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc1(csvEndPath1.string()); // Загружаем конечный CSV файл
        if (doc1.GetRowCount() == 0) { // Если файл пустой
            cpColumns(csvFirstPath1, csvEndPath1); // Копируем колонки из первого файла
        }

        // Формируем пути к первому и конечному CSV файлам второй таблицы
        fil::path csvFirstPath2 = fil::current_path() / jatab.scheme / table / "1.csv";
        fil::path csvEndPath2 = fil::current_path() / jatab.scheme / table / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc2(csvEndPath2.string()); // Загружаем конечный CSV файл второй таблицы
        if (doc2.GetRowCount() == 0) { // Если файл пустой
            cpColumns(csvFirstPath2, csvEndPath2); // Копируем колонки из первого файла
        }

        // Проверяем зависимость строки по условию
        if (isDependenceString(tableCond1, columnCond1, sCond, icsv, row, jatab)) {
            rapidcsv::Document docCond1(csvPathCond1.string()); // Загружаем CSV файл первой таблицы
            size_t columnsCount1 = docCond1.GetColumnCount(); // Получаем количество колонок
            ofstream csvResFile1(csvPathRes1, ios::app); // Открываем файл результата для добавления
            if (!csvResFile1.is_open()) { // Проверяем, удалось ли открыть файл
                cout << "Not open file: " << csvPathRes1 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            // Записываем данные в файл результата первой таблицы
            for (size_t j = 0; j < columnsCount1; j++) {
                if (j + 1 != columnsCount1) {
                    string currentCell = docCond1.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile1 << currentCell << ","; // Записываем значение с запятой
                } else {
                    string currentCell = docCond1.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile1 << currentCell; // Записываем последнее значение без запятой
                }
            }
            csvResFile1 << endl; // Переход на новую строку
            csvResFile1.close(); // Закрываем файл

            // Загружаем CSV файл второй таблицы
            fil::path csvPath2 = fil::current_path() / jatab.scheme / table / (to_string(icsv) + ".csv");
            rapidcsv::Document docCond2(csvPath2.string());
            size_t columnsCount2 = docCond2.GetColumnCount(); // Получаем количество колонок
            ofstream csvResFile2(csvPathRes2, ios::app); // Открываем файл результата второй таблицы для добавления
            if (!csvResFile2.is_open()) { // Проверяем, удалось ли открыть файл
                cout << "Not open file: " << csvPathRes2 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            // Записываем данные в файл результата второй таблицы
            for (size_t j = 0; j < columnsCount2; j++) {
                if (j + 1 != columnsCount2) {
                    string currentCell = docCond2.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile2 << currentCell << ","; // Записываем значение с запятой
                } else {
                    string currentCell = docCond2.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile2 << currentCell; // Записываем последнее значение без запятой
                }
            }
            csvResFile2 << endl; // Переход на новую строку
            csvResFile2.close(); // Закрываем файл
        }
        i++; // Увеличиваем индекс
    }
}

// Функция для обработки условия с использованием логического И
void condAnd(JsonTable& jatab, string tableCond1, string tableCond2, string columnCond1, string columnCond2, string tableCond3, string columnCond3, string sCond) {
    size_t i = 0; // Инициализируем индекс для итерации
    while (true) {
        // Вычисляем номер CSV файла и номер строки
        size_t icsv = i / jatab.rowsCount + 1;
        size_t row = i % jatab.rowsCount;

        // Формируем пути к CSV файлам для обеих таблиц
        fil::path csvPathCond1 = fil::current_path() / jatab.scheme / tableCond1 / (to_string(icsv) + ".csv");
        fil::path csvPathCond2 = fil::current_path() / jatab.scheme / tableCond2 / (to_string(icsv) + ".csv");
        ifstream fileCond1(csvPathCond1); // Открываем файл первой таблицы
        ifstream fileCond2(csvPathCond2); // Открываем файл второй таблицы
        if (!fileCond1.is_open() || !fileCond2.is_open()) { // Если хотя бы один файл не открыт
            break; // Выходим из цикла
        }
        fileCond1.close(); // Закрываем файл первой таблицы
        fileCond2.close(); // Закрываем файл второй таблицы

        // Формируем пути к файлам результатов
        fil::path csvPathRes1 = fil::current_path() / jatab.scheme / tableCond1 / ("res_" + to_string(icsv) + ".csv");
        fil::path csvPathRes2 = fil::current_path() / jatab.scheme / tableCond2 / ("res_" + to_string(icsv) + ".csv");

        // Проверяем, существует ли файл результата для первой таблицы
        ifstream fileRes1(csvPathRes1);
        if (!fileRes1.is_open()) { // Если файл не открыт
            // Создаем файл результата
            ofstream fileRes1Out(csvPathRes1);
            if (!fileRes1Out.is_open()) { // Если не удалось открыть файл
                cout << "Not open file: " << csvPathRes1 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            fileRes1Out.close(); // Закрываем файл
        } else {
            fileRes1.close(); // Закрываем файл
        }

        // Проверяем, существует ли файл результата для второй таблицы
        ifstream fileRes2(csvPathRes2);
        if (!fileRes2.is_open()) { // Если файл не открыт
            // Создаем файл результата
            ofstream fileRes2Out(csvPathRes2);
            if (!fileRes2Out.is_open()) { // Если не удалось открыть файл
                cout << "Not open file: " << csvPathRes2 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            fileRes2Out.close(); // Закрываем файл
        } else {
            fileRes2.close(); // Закрываем файл
        }

        // Формируем пути к первому и конечному CSV файлам первой таблицы
        fil::path csvFirstPath1 = fil::current_path() / jatab.scheme / tableCond1 / "1.csv";
        fil::path csvEndPath1 = fil::current_path() / jatab.scheme / tableCond1 / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc1(csvEndPath1.string()); // Загружаем конечный CSV файл
        if (doc1.GetRowCount() == 0) { // Если файл пустой
            cpColumns(csvFirstPath1, csvEndPath1); // Копируем колонки из первого файла
        }

        // Формируем пути к первому и конечному CSV файлам второй таблицы
        fil::path csvFirstPath2 = fil::current_path() / jatab.scheme / tableCond2 / "1.csv";
        fil::path csvEndPath2 = fil::current_path() / jatab.scheme / tableCond2 / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc2(csvEndPath2.string()); // Загружаем конечный CSV файл второй таблицы
        if (doc2.GetRowCount() == 0) { // Если файл пустой
            cpColumns(csvFirstPath2, csvEndPath2); // Копируем колонки из первого файла
        }

        // Проверяем зависимость таблиц и строк
        if (isDependenceTables(tableCond1, columnCond1, tableCond2, columnCond2, icsv, row, jatab) && isDependenceString(tableCond3, columnCond3, sCond, icsv, row, jatab)) {
            rapidcsv::Document docCond1(csvPathCond1.string()); // Загружаем CSV файл первой таблицы
            size_t columnsCount1 = docCond1.GetColumnCount(); // Получаем количество колонок
            ofstream csvResFile1(csvPathRes1, ios::app); // Открываем файл результата для добавления
            if (!csvResFile1.is_open()) { // Проверяем, удалось ли открыть файл
                cout << "Not open file: " << csvPathRes1 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            // Записываем данные в файл результата первой таблицы
            for (size_t j = 0; j < columnsCount1; j++) {
                if (j + 1 != columnsCount1) {
                    string currentCell = docCond1.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile1 << currentCell << ","; // Записываем значение с запятой
                } else {
                    string currentCell = docCond1.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile1 << currentCell; // Записываем последнее значение без запятой
                }
            }
            csvResFile1 << endl; // Переход на новую строку
            csvResFile1.close(); // Закрываем файл

            // Загружаем CSV файл второй таблицы
            rapidcsv::Document docCond2(csvPathCond2.string());
            size_t columnsCount2 = docCond2.GetColumnCount(); // Получаем количество колонок
            ofstream csvResFile2(csvPathRes2, ios::app); // Открываем файл результата второй таблицы для добавления
            if (!csvResFile2.is_open()) { // Проверяем, удалось ли открыть файл
                cout << "Not open file: " << csvPathRes2 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            // Записываем данные в файл результата второй таблицы
            for (size_t j = 0; j < columnsCount2; j++) {
                if (j + 1 != columnsCount2) {
                    string currentCell = docCond2.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile2 << currentCell << ","; // Записываем значение с запятой
                } else {
                    string currentCell = docCond2.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile2 << currentCell; // Записываем последнее значение без запятой
                }
            }
            csvResFile2 << endl; // Переход на новую строку
            csvResFile2.close(); // Закрываем файл
        }
        i++; // Увеличиваем индекс
    }
}

// Функция для обработки условия с использованием логического ИЛИ
void condOr(JsonTable& jatab, string tableCond1, string tableCond2, string columnCond1, string columnCond2, string tableCond3, string columnCond3, string sCond) {
    size_t i = 0; // Инициализируем индекс для итерации
    while (true) {
        // Вычисляем номер CSV файла и номер строки
        size_t icsv = i / jatab.rowsCount + 1;
        size_t row = i % jatab.rowsCount;

        // Формируем пути к CSV файлам для обеих таблиц
        fil::path csvPathCond1 = fil::current_path() / jatab.scheme / tableCond1 / (to_string(icsv) + ".csv");
        fil::path csvPathCond2 = fil::current_path() / jatab.scheme / tableCond2 / (to_string(icsv) + ".csv");
        ifstream fileCond1(csvPathCond1); // Открываем файл первой таблицы
        ifstream fileCond2(csvPathCond2); // Открываем файл второй таблицы
        if (!fileCond1.is_open() || !fileCond2.is_open()) { // Если хотя бы один файл не открыт
            break; // Выходим из цикла
        }
        fileCond1.close(); // Закрываем файл первой таблицы
        fileCond2.close(); // Закрываем файл второй таблицы

        // Формируем пути к файлам результатов
        fil::path csvPathRes1 = fil::current_path() / jatab.scheme / tableCond1 / ("res_" + to_string(icsv) + ".csv");
        fil::path csvPathRes2 = fil::current_path() / jatab.scheme / tableCond2 / ("res_" + to_string(icsv) + ".csv");

        // Проверяем, существует ли файл результата для первой таблицы
        ifstream fileRes1(csvPathRes1);
        if (!fileRes1.is_open()) { // Если файл не открыт
            // Создаем файл результата
            ofstream fileRes1Out(csvPathRes1);
            if (!fileRes1Out.is_open()) { // Если не удалось открыть файл
                cout << "Not open file: " << csvPathRes1 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            fileRes1Out.close(); // Закрываем файл
        } else {
            fileRes1.close(); // Закрываем файл
        }

        // Проверяем, существует ли файл результата для второй таблицы
        ifstream fileRes2(csvPathRes2);
        if (!fileRes2.is_open()) { // Если файл не открыт
            // Создаем файл результата
            ofstream fileRes2Out(csvPathRes2);
            if (!fileRes2Out.is_open()) { // Если не удалось открыть файл
                cout << "Not open file: " << csvPathRes2 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            fileRes2Out.close(); // Закрываем файл
        } else {
            fileRes2.close(); // Закрываем файл
        }

        // Формируем пути к первому и конечному CSV файлам первой таблицы
        fil::path csvFirstPath1 = fil::current_path() / jatab.scheme / tableCond1 / "1.csv";
        fil::path csvEndPath1 = fil::current_path() / jatab.scheme / tableCond1 / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc1(csvEndPath1.string()); // Загружаем конечный CSV файл
        if (doc1.GetRowCount() == 0) { // Если файл пустой
            cpColumns(csvFirstPath1, csvEndPath1); // Копируем колонки из первого файла
        }

        // Формируем пути к первому и конечному CSV файлам второй таблицы
        fil::path csvFirstPath2 = fil::current_path() / jatab.scheme / tableCond2 / "1.csv";
        fil::path csvEndPath2 = fil::current_path() / jatab.scheme / tableCond2 / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc2(csvEndPath2.string()); // Загружаем конечный CSV файл второй таблицы
        if (doc2.GetRowCount() == 0) { // Если файл пустой
            cpColumns(csvFirstPath2, csvEndPath2); // Копируем колонки из первого файла
        }

        // Проверяем зависимость таблиц и строк
        if (isDependenceTables(tableCond1, columnCond1, tableCond2, columnCond2, icsv, row, jatab) || isDependenceString(tableCond3, columnCond3, sCond, icsv, row, jatab)) {
            rapidcsv::Document docCond1(csvPathCond1.string()); // Загружаем CSV файл первой таблицы
            size_t columnsCount1 = docCond1.GetColumnCount(); // Получаем количество колонок
            ofstream csvResFile1(csvPathRes1, ios::app); // Открываем файл результата для добавления
            if (!csvResFile1.is_open()) { // Проверяем, удалось ли открыть файл
                cout << "Not open file: " << csvPathRes1 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            // Записываем данные в файл результата первой таблицы
            for (size_t j = 0; j < columnsCount1; j++) {
                if (j + 1 != columnsCount1) {
                    string currentCell = docCond1.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile1 << currentCell << ","; // Записываем значение с запятой
                } else {
                    string currentCell = docCond1.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile1 << currentCell; // Записываем последнее значение без запятой
                }
            }
            csvResFile1 << endl; // Переход на новую строку
            csvResFile1.close(); // Закрываем файл

            // Загружаем CSV файл второй таблицы
            rapidcsv::Document docCond2(csvPathCond2.string());
            size_t columnsCount2 = docCond2.GetColumnCount(); // Получаем количество колонок
            ofstream csvResFile2(csvPathRes2, ios::app); // Открываем файл результата второй таблицы для добавления
            if (!csvResFile2.is_open()) { // Проверяем, удалось ли открыть файл
                cout << "Not open file: " << csvPathRes2 << endl; // Сообщаем об ошибке
                return; // Завершаем выполнение функции
            }
            // Записываем данные в файл результата второй таблицы
            for (size_t j = 0; j < columnsCount2; j++) {
                if (j + 1 != columnsCount2) {
                    string currentCell = docCond2.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile2 << currentCell << ","; // Записываем значение с запятой
                } else {
                    string currentCell = docCond2.GetCell<string>(j, row); // Получаем значение ячейки
                    csvResFile2 << currentCell; // Записываем последнее значение без запятой
                }
            }
            csvResFile2 << endl; // Переход на новую строку
            csvResFile2.close(); // Закрываем файл
        }
        i++; // Увеличиваем индекс
    }
}

// Функция для проверки наличия точки в строке
bool isDot(string mes) {
    // Проходим по всем символам в строке
    for (size_t i = 0; i < mes.size(); i++) {
        // Если находим точку, возвращаем true
        if (mes[i] == '.') {
            return true;
        }
    }
    // Если точка не найдена, возвращаем false
    return false;
}

// Функция для проверки зависимости между таблицами
bool isDependenceTables(string table, string column, string tableCond, string columnCond, size_t csvNum, size_t row, JsonTable& jatab) {
    // Формируем путь к CSV файлу основной таблицы
    fil::path csvPath = fil::current_path() / jatab.scheme / table / (to_string(csvNum) + ".csv");
    // Формируем путь к CSV файлу условной таблицы
    fil::path csvPathCond = fil::current_path() / jatab.scheme / tableCond / (to_string(csvNum) + ".csv");
    
    // Загружаем документы из CSV файлов
    rapidcsv::Document doc(csvPath.string());
    rapidcsv::Document docCond(csvPathCond.string());
    
    // Получаем индексы колонок по названиям
    int idxColumn = doc.GetColumnIdx(column);
    int idxColumnCond = docCond.GetColumnIdx(columnCond);
    
    // Получаем количество строк в обоих документах
    int rowsCount = doc.GetRowCount();
    int rowsCountCond = docCond.GetRowCount();
    
    // Проверяем, что запрашиваемая строка существует в обоих документах
    if (row >= rowsCount || row >= rowsCountCond) return false;
    
    // Сравниваем значения в указанных строках и колонках
    if (doc.GetCell<string>(idxColumn, row) == docCond.GetCell<string>(idxColumnCond, row)) return true;
    else return false; // Если значения не равны, возвращаем false
}

// Функция для проверки зависимости строки от условия
bool isDependenceString(string table, string column, string sCond, size_t csvNum, size_t row, JsonTable& jatab) {
    // Формируем путь к CSV файлу
    fil::path csvPath = fil::current_path() / jatab.scheme / table / (to_string(csvNum) + ".csv");
    
    // Загружаем документ из CSV файла
    rapidcsv::Document doc(csvPath.string());
    
    // Получаем индекс колонки по названию
    int idxColumn = doc.GetColumnIdx(column);
    
    // Получаем количество строк в документе
    int rowsCount = doc.GetRowCount();
    
    // Проверяем, что запрашиваемая строка существует
    if (row >= rowsCount) return false;
    
    // Сравниваем значение в указанной строке с условием
    if (doc.GetCell<string>(idxColumn, row) == sCond) return true;
    else return false; // Если значения не равны, возвращаем false
}