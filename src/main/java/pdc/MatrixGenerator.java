package pdc;

import java.util.Random;

/**
 * Utility class for generating and manipulating matrices.
 * Provides helper methods for creating test and example matrices.
 */
public class MatrixGenerator {

    private static final Random random = new Random();

    /**
     * Generates a random matrix of specified dimensions.
     * 
     * @param rows     number of rows
     * @param cols     number of columns
     * @param maxValue maximum value for matrix elements (exclusive)
     * @return a randomly generated matrix
     */
    public static int[][] generateRandomMatrix(int rows, int cols, int maxValue) {
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = random.nextInt(maxValue);
            }
        }
        return matrix;
    }

    /**
     * Generates an identity matrix of specified size.
     * 
     * @param size the dimension of the identity matrix
     * @return an identity matrix
     */
    public static int[][] generateIdentityMatrix(int size) {
        int[][] matrix = new int[size][size];
        for (int i = 0; i < size; i++) {
            matrix[i][i] = 1;
        }
        return matrix;
    }

    /**
     * Generates a matrix filled with a specific value.
     * 
     * @param rows  number of rows
     * @param cols  number of columns
     * @param value the value to fill the matrix with
     * @return a matrix filled with the specified value
     */
    public static int[][] generateFilledMatrix(int rows, int cols, int value) {
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = value;
            }
        }
        return matrix;
    }

    /**
     * Prints a matrix to console in a readable format.
     * 
     * @param matrix the matrix to print
     * @param label  optional label for the matrix
     */
    public static void printMatrix(int[][] matrix, String label) {
        if (label != null && !label.isEmpty()) {
            System.out.println(label);
        }
        for (int[] row : matrix) {
            for (int val : row) {
                System.out.printf("%6d ", val);
            }
            System.out.println();
        }
    }

    /**
     * Prints a matrix to console without a label.
     * 
     * @param matrix the matrix to print
     */
    public static void printMatrix(int[][] matrix) {
        printMatrix(matrix, "");
    }

    /**
     * Multiplies two matrices (for verification of distributed results).
     * 
     * @param a first matrix
     * @param b second matrix
     * @return the product matrix a * b
     * @throws IllegalArgumentException if matrix dimensions are incompatible
     */
    public static int[][] multiply(int[][] a, int[][] b) {
        if (a == null || b == null || a.length == 0 || b.length == 0) {
            return new int[0][0];
        }
        
        int rowsA = a.length;
        int colsA = a[0].length;
        int colsB = b[0].length;
        
        if (colsA != b.length) {
            throw new IllegalArgumentException(
                "Matrix dimensions don't match: " + rowsA + "x" + colsA + 
                " and " + b.length + "x" + colsB
            );
        }
        
        int[][] result = new int[rowsA][colsB];
        
        for (int i = 0; i < rowsA; i++) {
            for (int j = 0; j < colsB; j++) {
                int sum = 0;
                for (int k = 0; k < colsA; k++) {
                    sum += a[i][k] * b[k][j];
                }
                result[i][j] = sum;
            }
        }
        
        return result;
    }

    /**
     * Compare two matrices for equality.
     * 
     * @param a first matrix
     * @param b second matrix
     * @return true if matrices are equal, false otherwise
     */
    public static boolean areEqual(int[][] a, int[][] b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        if (a.length != b.length) return false;
        
        for (int i = 0; i < a.length; i++) {
            if (a[i].length != b[i].length) return false;
            for (int j = 0; j < a[i].length; j++) {
                if (a[i][j] != b[i][j]) return false;
            }
        }
        
        return true;
    }

    /**
     * Clone a matrix.
     * 
     * @param matrix the matrix to clone
     * @return a deep copy of the matrix
     */
    public static int[][] clone(int[][] matrix) {
        if (matrix == null) return null;
        
        int[][] copy = new int[matrix.length][];
        for (int i = 0; i < matrix.length; i++) {
            copy[i] = matrix[i].clone();
        }
        return copy;
    }

    /**
     * Transpose a matrix.
     * 
     * @param matrix the matrix to transpose
     * @return the transposed matrix
     */
    public static int[][] transpose(int[][] matrix) {
        if (matrix == null || matrix.length == 0) return new int[0][0];
        
        int rows = matrix.length;
        int cols = matrix[0].length;
        int[][] transposed = new int[cols][rows];
        
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                transposed[j][i] = matrix[i][j];
            }
        }
        
        return transposed;
    }

    /**
     * Generate a random square matrix.
     * 
     * @param size the size of the square matrix
     * @param maxValue maximum value for elements
     * @return a random square matrix
     */
    public static int[][] generateSquareMatrix(int size, int maxValue) {
        return generateRandomMatrix(size, size, maxValue);
    }

    /**
     * Generate a matrix with sequential values (for testing).
     * 
     * @param rows number of rows
     * @param cols number of columns
     * @return a matrix with sequential values starting from 1
     */
    public static int[][] generateSequentialMatrix(int rows, int cols) {
        int[][] matrix = new int[rows][cols];
        int value = 1;
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = value++;
            }
        }
        return matrix;
    }

    /**
     * Print matrix dimensions.
     * 
     * @param matrix the matrix
     */
    public static void printDimensions(int[][] matrix) {
        if (matrix == null || matrix.length == 0) {
            System.out.println("Empty matrix");
            return;
        }
        System.out.println("Dimensions: " + matrix.length + " x " + matrix[0].length);
    }

    /**
     * Example usage and testing.
     */
    public static void main(String[] args) {
        System.out.println("=== MatrixGenerator Test ===\n");
        
        // Generate small test matrices
        int[][] a = generateRandomMatrix(3, 3, 10);
        int[][] b = generateRandomMatrix(3, 3, 10);
        
        System.out.println("Matrix A:");
        printMatrix(a);
        
        System.out.println("\nMatrix B:");
        printMatrix(b);
        
        System.out.println("\nA * B:");
        int[][] result = multiply(a, b);
        printMatrix(result);
        
        // Test identity matrix
        System.out.println("\nIdentity Matrix (4x4):");
        printMatrix(generateIdentityMatrix(4));
        
        // Test sequential matrix
        System.out.println("\nSequential Matrix (3x4):");
        printMatrix(generateSequentialMatrix(3, 4));
        
        // Test equality
        int[][] copy = clone(a);
        System.out.println("\nAre A and its clone equal? " + areEqual(a, copy));
        
        // Test transpose
        System.out.println("\nTranspose of A:");
        printMatrix(transpose(a));
    }
}