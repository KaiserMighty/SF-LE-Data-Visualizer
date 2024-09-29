#ifndef LAWPROCESSOR_CSV_H
#define LAWPROCESSOR_CSV_H

char ** csvopen (char * filename);
char ** csvnext (void);
char ** csvheader (void);
int csvclose (void);

#endif