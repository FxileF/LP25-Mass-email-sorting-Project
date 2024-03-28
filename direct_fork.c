//
// Created by flassabe on 26/10/22.
//

#include "direct_fork.h"

#include <limits.h>
#include <dirent.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "analysis.h"
#include "utility.h"


/*!
 * @brief direct_fork_directories runs the directory analysis with direct calls to fork
 * @param data_source the data source directory with 150 directories to analyze (parallelize with fork)
 * @param temp_files the path to the temporary files directory
 * @param nb_proc the maximum number of simultaneous processes
 */
void direct_fork_directories(char *data_source, char *temp_files, uint16_t nb_proc) {
    // 1. Check parameters
    if (data_source == NULL || temp_files == NULL || nb_proc == 0) {
        return;
    }

    // 2. Iterate over directories (ignore . and ..)
    DIR *dir = opendir(data_source);
    if (dir == NULL) {
        return;
    }

    int running_processes = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        // if max processes count already run, wait for one to end before starting a task.
        while (running_processes >= nb_proc) {
            wait(NULL);
            running_processes--;
        }

        // 3. fork and start a task on current directory.
        pid_t pid = fork();
        if (pid == 0) {
            // Create task
            task_t task;
            task.task_callback = process_directory;
            directory_task_t *dir_task = (directory_task_t *) &task;
            snprintf(dir_task->object_directory, 255, "%s/%s", data_source, entry->d_name);
            strncpy(dir_task->temporary_directory, temp_files, STR_MAX_LEN);
            dir_task->task_callback(&task);


            exit(0);
        } else if (pid > 0) {
            running_processes++;
        }

    }
    // 4. Cleanup
    closedir(dir);

    // Wait for remaining processes to finish
    while (running_processes > 0) {
        wait(NULL);
        running_processes--;
    }
}

/*!
 * @brief direct_fork_files runs the files analysis with direct calls to fork
 * @param data_source the data source containing the files
 * @param temp_files the temporary files to write the output (step2_output)
 * @param nb_proc the maximum number of simultaneous processes
 */
void direct_fork_files(char *data_source, char *temp_files, uint16_t nb_proc) {
    // 1. Check parameters
    if (data_source == NULL || temp_files == NULL || nb_proc == 0) return;

    // 2. Iterate over files in files list (step1_output)
    char step1_output[255];
    snprintf(step1_output, 255, "%s/%s", temp_files, "step1_output");
    FILE *input_file = fopen(step1_output, "r");
    if (input_file == NULL) return;

    char file[STR_MAX_LEN];
    int running_processes = 0;

    task_t task;
    task.task_callback = process_file;
    file_task_t *file_task = (file_task_t *) &task;
    strcpy(file_task->temporary_directory, temp_files);
    while (fgets(file, STR_MAX_LEN, input_file) != NULL) {

        file[strlen(file) - 1] = '\0'; // Remove new line character

        strcpy(file_task->object_file, file);

        file_task->task_callback(&task);


/*
        while (running_processes >= nb_proc) {
            wait(NULL);
            running_processes--;
        }

        int pid = fork();
        if (pid == 0) {
            file_task->task_callback(&task);
            exit(0);
        } else if (pid > 0) {
            running_processes++;
        }else{
            perror("Error calling fork");
        }
*/
    }
    // 4. Cleanup
    fclose(input_file);

    // Wait for remaining processes to finish
    while (running_processes > 0) {
        wait(NULL);
        running_processes--;
    }
}


