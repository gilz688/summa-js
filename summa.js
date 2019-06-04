const klyng = require('klyng');
const Logger = require('js-logger');

const n = 4;
const MIN = -100;
const MAX = 100;

Logger.useDefaults();
const DEBUG = true;
if(!DEBUG) {
    Logger.setLevel(Logger.INFO);
}

function summa(rank, row, col, processRows, processCols) {
    /* create matrices */
    let localA = klyng.recv({ from: 0, subject: 'localA' });
    let localB = klyng.recv({ from: 0, subject: 'localB' });
    let localC = createMatrix(localA.length, localB.length, zero);

    let tempA = createMatrix(localA.length, localA.length, zero);
    let tempB = createMatrix(localB.length, localB.length, zero);

    /* compute */
    for (let k = 0; k < processCols; k++) {
        let subjectA = "A[" + row + "][" + k + "]";
        if (col == k) {
            tempA = localA;
            broadcastRow(tempA, rank, row, processCols, subjectA);
        } else {
            Logger.debug(rank + ": received " + subjectA);
            tempA = klyng.recv({ subject: subjectA });
        }
        
        let subjectB = "B[" + k + "][" + col + "]";
        if (row == k) {
            tempB = localB;
            broadcastCol(tempB, rank, col, processRows, subjectB);
        } else {
            Logger.debug(rank + ": received " + subjectB);
            tempB = klyng.recv({ subject: subjectB });
        }

        Logger.debug(rank + ": localC += " + subjectA + " x " + subjectB);
        naiveMatrixMult(tempA, tempB, localC);
    }

    Logger.debug(rank + ": localC =\n" + matrixToString(localC));
    /* send localC to root process for displaying the entire matrix C */
    klyng.send({ to: 0, subject: 'localC', data: localC });
}

function main() {
    let size = klyng.size();
    let rank = klyng.rank();

    let processRows = Math.sqrt(size);
    let processCols = processRows;

    if (processCols * processRows !== size) {
        Logger.error('ERROR: number of processes must be a perfect square!');
        klyng.end();
    }

    if (rank === 0) {
        Logger.info('Generating matrix A...');
        const A = createMatrix(n, n, randomPositiveInt);
        Logger.info('Generating matrix B...');
        const B = createMatrix(n, n, identity);
        let C = createMatrix(n, n, zero);

        const subMatrixRows = n / processRows;
        const subMatrixColumns = n / processCols;

        Logger.debug('A =');
        Logger.debug(matrixToString(A));
        Logger.debug('B =');
        Logger.debug(matrixToString(B));

        let expectedC = createMatrix(n, n, zero);
        naiveMatrixMult(A, B, expectedC);
        Logger.debug('expectedC =');
        Logger.debug(matrixToString(expectedC));

        let hrstart = process.hrtime();

        Logger.info('Partitioning matrices...');
        // broadcast sub-matrices to each process
        for (let i = 0; i < processRows; i++) {
            for (let j = 0; j < processCols; j++) {
                let p = i * processRows + j;
                let x = j * subMatrixColumns;
                let y = i * subMatrixRows;
                let processMatrixA = subMatrix(A, x, y, subMatrixColumns, subMatrixRows);
                let processMatrixB = subMatrix(B, x, y, subMatrixColumns, subMatrixRows);
                console.log(p + ": A[" + i + "]["+ j + "] =\n" + matrixToString(processMatrixA));
                console.log(p + ": B[" + i + "]["+ j + "] =\n" + matrixToString(processMatrixB));
                klyng.send({ to: p, subject: 'localA', data: processMatrixA });
                klyng.send({ to: p, subject: 'localB', data: processMatrixB });
            }
        }

        summa(rank, 0, 0, processRows, processCols);

        // here the root will wait for other processes
        for (let i = 0; i < processRows; i++) {
            for (let j = 0; j < processCols; j++) {
                let p = i * processRows + j;
                let x = j * subMatrixColumns;
                let y = i * subMatrixRows;

                let proccessMatrixC = klyng.recv({ from: p, subject: 'localC' });
                mergeMatrix(C, proccessMatrixC, x, y, subMatrixColumns, subMatrixRows);
            }
        }

        let hrend = process.hrtime(hrstart);
        Logger.info("Execution time (hr): %ds %dms", hrend[0], hrend[1]/1000000);
        Logger.debug('\nC =');
        Logger.debug(matrixToString(C));

        Logger.info('match: ' + compareMatrix(C, expectedC, 0.001));
    } else {
        summa(rank, Math.floor(rank / processRows), rank % processRows, processRows, processCols);
    }

    klyng.end();
}

function broadcastRow(data, rank, row, processCols, subject) {
    for (let y = 0; y < processCols; y++) {
        let p = row * processCols + y;
        if (p != rank) {
            klyng.send({ to: p, subject: subject, data: data });
            Logger.debug(rank + ": " + subject + " -> " + p);
        }
    }
}

function broadcastCol(data, rank, col, processRows, subject) {
    for (let x = 0; x < processRows; x++) {
        let p = x * processRows + col;
        if (p != rank) {
            klyng.send({ to: p, subject: subject, data: data });
            Logger.debug(rank + ": " + subject + " -> " + p);
        }
    }
}

// extracts sub-matrix
function subMatrix(matrix, x, y, cols, rows) {
    let newMatrix = createMatrix(rows, cols, zero);
    for (let i = 0; i < rows; i++) {
        for (let j = 0; j < cols; j++) {
            newMatrix[i][j] = matrix[y + i][x + j];
        }
    }
    return newMatrix;
}

// merge sub-matrix
function mergeMatrix(matrix, subMatrix, x, y, cols, rows) {
    for (let i = 0; i < rows; i++) {
        for (let j = 0; j < cols; j++) {
            matrix[y + i][x + j] = subMatrix[i][j];
        }
    }
}

// set matrixB to matrix A
function setMatrix(matrixA, matrixB, cols, rows) {
    for (let i = 0; i < rows; i++) {
        for (let j = 0; j < cols; j++) {
            matrixA[i][j] = matrixB[i][j];
        }
    }
}

// compares matrix a with matrix e
function compareMatrix(a, e, maxError) {
    if (a.length !== e.length) {
        return false;
    }

    let len = a.length;
    for (let i = 0; i < len; i++) {
        for (let j = 0; j < len; j++) {
            let error = Math.abs(a[j][i] - e[j][i]) / Math.abs(e[j][i]);
            if (error > maxError) {
                return false;
            }
        }
    }
    return true;
}

// creates a nxm matrix using generator function
function createMatrix(n, m, generator) {
    let arr = [];
    for (let i = 0; i < n; i++) {
        let columns = [];
        for (let j = 0; j < m; j++) {
            columns[j] = generator(i, j);
        }
        arr[i] = columns;
    }
    return arr;
}

// converts matrix to string
function matrixToString(matrix) {
    if(!DEBUG) return '';
    let string = '';
    let y = matrix.length;
    for (let i = 0; i < y; i++) {
        let x = matrix[i].length;
        string += '|\t';
        for (let j = 0; j < x; j++) {
            string += matrix[i][j].toString();
            if (j < x - 1) string += '\t';
        }
        string += '\t|';
        if (i < x) string += '\n';
    }
    return string;
}

// matrix addition
function addMatrix(A, B) {
    let len = A.length;

    if (A.length !== B.length) {
        return null;
    }

    let C = createMatrix(len, len, zero);
    for (let i = 0; i < n; i++) {
        for (let j = 0; j < n; j++) {
            C[i][j] = A[i][j] + B[i][j];
        }
    }
    return C;
}

// used for checking
function naiveMatrixMult(A, B, C) {
    let len = A.length;

    if (A.length !== B.length) {
        return null;
    }

    for (let i = 0; i < len; i++) {
        for (let j = 0; j < len; j++) {
            for (let k = 0; k < len; k++) {
                C[i][j] += A[i][k] * B[k][j];
            }
        }
    }
}

// generator functions for generateSquareMatrix()

function zero() {
    return 0;
}

function randomNumber() {
    return Math.random() * (MAX - MIN) + MIN;
}

function randomInt() {
    return Math.ceil(Math.random() * (MAX - MIN) + MIN);
}

function randomPositiveInt() {
    return Math.ceil(Math.random() * (MAX - 1) + 1);
}

function identity(j, i) {
    return j === i ? 1 : 0;
}

klyng.init(main);