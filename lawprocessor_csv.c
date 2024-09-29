#include "lawprocessor_csv.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define BUFFERSIZE 512

FILE* csv;
char** header;
int linesRead;
int fileOpen;
pthread_mutex_t getLine;
pthread_mutex_t linesReadMutex;

char ** csvopen (char * filename)
{
    //Initialize Mutexes
    if (pthread_mutex_init(&getLine, NULL) != 0)
    {
        perror("Mutex initialization failed");
        exit(EXIT_FAILURE);
    }
    if (pthread_mutex_init(&linesReadMutex, NULL) != 0)
    {
        perror("Mutex initialization failed");
        exit(EXIT_FAILURE);
    }
    //Open file and initialize global variables
    csv = fopen(filename, "r");
    if (csv == NULL)
    {
        perror("Error Opening File");
        return NULL;
    }
    header = csvnext();
    linesRead = 0;
    fileOpen = 1;
    return header;
}

char ** csvnext (void)
{
    //Create input and output buffers
    char nextLine[BUFFERSIZE];
    char** parsedLine = (char**) malloc(BUFFERSIZE);
    if (parsedLine == NULL)
    {
        perror("Malloc Failure");
        exit(EXIT_FAILURE);
    }
    //Create field(s) buffer
    char cellValue[BUFFERSIZE];
    int cellIndex = 0;
    int cellCount = 0;
    //Create flags for handling quotes and newlines
    int insideQuotes = 0;
    int completeLine = 0;
    while (!completeLine)
    {
        //Read a CSV line safely (threading)
        pthread_mutex_lock(&getLine);
        if (fgets(nextLine, BUFFERSIZE, csv) == NULL)
        {
            pthread_mutex_unlock(&getLine);
            break;
        }
        pthread_mutex_unlock(&getLine);
        //Process the line character-by-character
        for (int i = 0; i < strlen(nextLine)+1; i++)
        {
            if (nextLine[i] == '"' && !insideQuotes)
            {
                insideQuotes = 1;
            }
            else if (nextLine[i] == '"' && insideQuotes)
            {
                //End of file
                if (nextLine[i+1] == EOF)
                {
                    completeLine = 1;
                    break;
                }
                //Escaped quote
                else if (nextLine[i+1] == '"')
                {
                    cellValue[cellIndex++] = '"';
                }
                //End of quoted field
                else
                {
                    insideQuotes = 0;
                }
            }
            //End of field
            else if (nextLine[i] == ',' && !insideQuotes)
            {
                cellValue[cellIndex] = '\0';
                parsedLine[cellCount++] = strdup(cellValue);
                cellIndex = 0;
            }
            //End of record
            else if (nextLine[i] == '\n' && !insideQuotes)
            {
                cellValue[cellIndex] = '\0';
                parsedLine[cellCount++] = strdup(cellValue);
                cellIndex = 0;
                completeLine = 1;
                break;
            }
            //Populate field
            else
            {
                if (cellIndex < BUFFERSIZE - 1)
                {
                    cellValue[cellIndex++] = nextLine[i];
                }
            }
        }
    }
    //If the record is empty, do not return the array
    if (cellCount == 0)
    {
        free(parsedLine);
        parsedLine = NULL;
        return NULL;
    }
    parsedLine[cellCount] = NULL; //Add null terminator
    pthread_mutex_lock(&linesReadMutex);
    linesRead++;
    pthread_mutex_unlock(&linesReadMutex);
    return parsedLine;
}

char ** csvheader (void)
{
    return header;
}

int csvclose (void)
{
    pthread_mutex_destroy(&getLine);
    pthread_mutex_destroy(&linesReadMutex);
    if (!fileOpen)
    {
        return -1;
    }
    if (fclose(csv) != 0)
    {
        perror("Error Closing File");
        exit(EXIT_FAILURE);
    }
    //Free header memory
    for (int i = 0; header[i] != NULL; i++)
    {
        free(header[i]);
        header[i] = NULL;
    }
    free(header);
    header = NULL;
    return linesRead;
}