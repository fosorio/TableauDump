import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook; // Handles both XLSX and XLSB

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class XlsbKeyValueParser {

    public static void main(String[] args) {
        String filePath = "sample_kv.xlsb"; // Change this to your file's path
        Map<String, String> keyValueData = new HashMap<>();

        try (FileInputStream fis = new FileInputStream(new File(filePath));
             Workbook workbook = new XSSFWorkbook(fis)) {

            if (workbook.getNumberOfSheets() == 0) {
                System.err.println("The workbook is empty (no sheets found).");
                return;
            }

            // Get the first sheet
            Sheet sheet = workbook.getSheetAt(0);
            System.out.println("Reading data from sheet: " + sheet.getSheetName());
            System.out.println("------------------------------------");

            DataFormatter dataFormatter = new DataFormatter();
            FormulaEvaluator formulaEvaluator = workbook.getCreationHelper().createFormulaEvaluator();

            Iterator<Row> rowIterator = sheet.iterator();

            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();

                Cell keyCell = row.getCell(0);   // Column A (index 0) for Key
                Cell valueCell = row.getCell(1); // Column B (index 1) for Value

                String key = null;
                String value = ""; // Default to empty string if value cell is missing or blank

                if (keyCell != null) {
                    key = getCellValueAsString(keyCell, dataFormatter, formulaEvaluator);
                }

                if (valueCell != null) {
                    value = getCellValueAsString(valueCell, dataFormatter, formulaEvaluator);
                }

                // Only add to map if the key is not null and not an empty/blank string
                if (key != null && !key.trim().isEmpty()) {
                    keyValueData.put(key, value);
                } else {
                    // Optional: Log or handle rows where the key is missing/blank
                    if (row.getRowNum() > 0 || (value != null && !value.trim().isEmpty())) { // Avoid logging truly empty rows
                        System.out.println("Skipping row " + (row.getRowNum() + 1) + " due to missing or blank key.");
                    }
                }
            }

            // Print the extracted key-value pairs
            System.out.println("\nExtracted Key-Value Pairs:");
            for (Map.Entry<String, String> entry : keyValueData.entrySet()) {
                System.out.println("Key: \"" + entry.getKey() + "\", Value: \"" + entry.getValue() + "\"");
            }

        } catch (IOException e) {
            System.err.println("Error reading the XLSB file: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("An unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String getCellValueAsString(Cell cell, DataFormatter dataFormatter, FormulaEvaluator formulaEvaluator) {
        if (cell == null) {
            return ""; // Treat null cells as empty strings
        }

        // Use DataFormatter to get the cell value as a String, respecting Excel's formatting
        // and evaluating formulas if a formulaEvaluator is provided.
        String cellValue = dataFormatter.formatCellValue(cell, formulaEvaluator);

        // If it's a formula cell and dataFormatter returned the formula string itself,
        // you might want to force evaluation again, though usually dataFormatter handles it.
        // This part is more robust if you absolutely need the evaluated value and suspect
        // dataFormatter might not be enough for some edge cases.
        if (cell.getCellType() == CellType.FORMULA) {
            try {
                CellValue evaluatedCellValue = formulaEvaluator.evaluate(cell);
                switch (evaluatedCellValue.getCellType()) {
                    case BOOLEAN:
                        return String.valueOf(evaluatedCellValue.getBooleanValue());
                    case NUMERIC:
                        // Special handling for dates if DataFormatter didn't format it as desired
                        if (DateUtil.isCellDateFormatted(cell)) {
                            // You can use a specific date format or rely on DataFormatter
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // Example
                            return sdf.format(cell.getDateCellValue());
                        } else {
                            return String.valueOf(evaluatedCellValue.getNumberValue());
                        }
                    case STRING:
                        return evaluatedCellValue.getStringValue();
                    case BLANK:
                        return "";
                    case ERROR:
                        return "ERROR: " + FormulaError.forInt(evaluatedCellValue.getErrorValue()).getString();
                    default:
                        return cellValue; // Fallback to DataFormatter's output
                }
            } catch (Exception e) {
                // If formula evaluation fails, return the formula string or an error message
                return cell.getCellFormula(); // Or "Error evaluating formula"
            }
        }
        return cellValue;
    }
}
