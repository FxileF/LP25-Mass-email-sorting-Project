//
// Created by flassabe on 27/10/22.
//

#include "fifo_processes.h"

#include "global_defs.h"
#include <malloc.h>
#include <sys/stat.h>
#include <signal.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/wait.h>
#include <string.h>
#include <fcntl.h>

#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <unistd.h>

#include "analysis.h"
#include "utility.h"

/*!
 * @brief make_fifos creates FIFOs for processes to communicate with their parent
 * @param processes_count the number of FIFOs to create
 * @param file_format the filename format, e.g. fifo-out-%d, used to name the FIFOs
 */
void make_fifos(uint16_t processes_count, char *file_format) {
    char *filename;
    int i;
    for (i = 0; i < processes_count; i++) {
        // Create a buffer large enough to hold the file name
        filename = malloc(strlen(file_format) + 20);
        sprintf(filename, file_format, i);

        // Create the FIFO file with read/write permissions for the owner
        if (mkfifo(filename, 0666) < 0) {
            perror("mkfifo");
            exit(1);
        }

        // Free the filename buffer
        free(filename);
    }
}

/*!
 * @brief erase_fifos erases FIFOs used for processes communications with the parent
 * @param processes_count the number of FIFOs to destroy
 * @param file_format the filename format, e.g. fifo-out-%d, used to name the FIFOs
 */
void erase_fifos(uint16_t processes_count, char *file_format) {
    char *filename;
    int i;
    for (i = 0; i < processes_count; i++) {
        // Create a buffer large enough to hold the file name
        filename = malloc(strlen(file_format) + 20);
        sprintf(filename, file_format, i);

        // Delete the FIFO file
        if (unlink(filename) < 0) {
            perror("unlink");
            exit(1);
        }

        // Free the filename buffer
        free(filename);
    }
}

/*!
 * @brief make_processes creates processes and starts their code (waiting for commands)
 * @param processes_count the number of processes to create
 * @return a malloc'ed array with the PIDs of the created processes
 */
pid_t *make_processes(uint16_t processes_count) {
    pid_t *pids;
    int i;

    // Allocate an array to hold the PIDs
    pids = malloc(sizeof(pid_t) * processes_count);
    if (pids == NULL) {
        perror("malloc");
        exit(1);
    }

    for (i = 0; i < processes_count; i++) {
        pids[i] = fork();
        if (pids[i] == 0) {
            // This is the child process

            // Open the FIFOs
            char in_fifo_name[1024];
            char out_fifo_name[1024];
            sprintf(in_fifo_name, "fifo-in-%d", i);
            sprintf(out_fifo_name, "fifo-out-%d", i);
            int in_fifo = open(in_fifo_name, O_RDONLY);
            int out_fifo = open(out_fifo_name, O_WRONLY);
            if (in_fifo < 0 || out_fifo < 0) {
                perror("open");
                exit(1);
            }

            // Listen for tasks on the input FIFO
            task_t task;
            while (1) {
                if (read(in_fifo, &task, sizeof(task_t)) < 0) {
                    perror("read");
                    exit(1);
                }

                if (task.task_callback == NULL) {
                    // Shutdown task received, exit the loop
                    break;
                }

                // Apply the task
                task.task_callback(&task);

                // Write a notification to the output FIFO to signal that the task has been completed
                char notification[1024] = "Task completed";
                if (write(out_fifo, &notification, sizeof(notification)) < 0) {
                    perror("write");
                    exit(1);
                }
            }


            // Cleanup and exit
            close(in_fifo);
            close(out_fifo);
            exit(0);
        } else if (pids[i] < 0) {
            // fork failed
            perror("fork");
            exit(1);
        }
    }

    return pids;
}

/*!
 * @brief open_fifos opens FIFO from the parent's side
 * @param processes_count the number of FIFOs to open (must be created before)
 * @param file_format the name pattern of the FIFOs
 * @param flags the opening mode for the FIFOs
 * @return a malloc'ed array of opened FIFOs file descriptors
 */
int *open_fifos(uint16_t processes_count, char *file_format, int flags) {
    char *filename;
    int *fds;
    int i;

    // Allocate an array to hold the file descriptors
    fds = malloc(sizeof(int) * processes_count);
    if (fds == NULL) {
        perror("malloc");
        exit(1);
    }

    for (i = 0; i < processes_count; i++) {
        // Create a buffer large enough to hold the file name
        filename = malloc(strlen(file_format) + 20);
        sprintf(filename, file_format, i);

        // Open the FIFO file
        fds[i] = open(filename, flags);
        if (fds[i] < 0) {
            perror("open");
            exit(1);
        }

        // Free the filename buffer
        free(filename);
    }

    return fds;
}

/*!
 * @brief close_fifos closes FIFOs opened by the parent
 * @param processes_count the number of FIFOs to close
 * @param files the array of opened FIFOs as file descriptors
 */
void close_fifos(uint16_t processes_count, int *files) {
    int i;
    for (i = 0; i < processes_count; i++) {
        // Close the FIFO file
        if (close(files[i]) < 0) {
            perror("close");
            exit(1);
        }
    }
}

/*!
 * @brief shutdown_processes terminates all worker processes by sending a task with a NULL callback
 * @param processes_count the number of processes to terminate
 * @param fifos the array to the output FIFOs (used to command the processes) file descriptors
 */
void shutdown_processes(uint16_t processes_count, int *fifos) {
    task_t task;
    int i;

    // Set the callback to NULL
    task.task_callback = NULL;

    for (i = 0; i < processes_count; i++) {
        // Send the task to the current process
        if (write(fifos[i], &task, sizeof(task_t)) < 0) {
            perror("write");
            exit(1);
        }
    }
}

/*!
 * @brief prepare_select prepares fd_set for select with all file descriptors to look at
 * @param fds the fd_set to initialize
 * @param filesdes the array of file descriptors
 * @param nb_proc the number of processes (elements in the array)
 * @return the maximum file descriptor value (as used in select)
 */
int prepare_select(fd_set *fds, const int *filesdes, uint16_t nb_proc) {
    int i;
    int maxfd = -1;

    // Initialize the fd_set
    FD_ZERO(fds);

    // Add the file descriptors to the fd_set
    for (i = 0; i < nb_proc; i++) {
        FD_SET(filesdes[i], fds);
        if (filesdes[i] > maxfd) {
            maxfd = filesdes[i];
        }
    }

    return maxfd;
}

/*!
 * @brief send_task sends a directory task to a child process. Must send a directory command on object directory
 * data_source/dir_name, to write the result in temp_files/dir_name. Sends on FIFO with FD command_fd
 * @param data_source the data source with directories to analyze
 * @param temp_files the temporary output files directory
 * @param dir_name the current dir name to analyze
 * @param command_fd the child process command FIFO file descriptor
 */
void send_task(char *data_source, char *temp_files, char *dir_name, int command_fd) {
    directory_task_t task;

    // Set the callback function and the task arguments
    task.task_callback = &process_directory;
    snprintf(task.object_directory, STR_MAX_LEN, "%s/%s", data_source, dir_name);
    strcpy(task.temporary_directory,temp_files);

    // Send the task to the child process
    if (write(command_fd, &task, sizeof(directory_task_t)) < 0) {
        perror("write");
        exit(1);
    }
}

void send_task_file(char *data_source, char *temp_files, char *file_name, int command_fd) {
    file_task_t task;

    // Set the callback function and the task arguments
    task.task_callback = &process_file;
    strcpy(task.object_file,file_name);
    strcpy(task.temporary_directory,temp_files);

    // Send the task to the child process
    if (write(command_fd, &task, sizeof(file_task_t)) < 0) {
        perror("write");
        exit(1);
    }
}

/*!
 * @brief fifo_process_directory is the main function to distribute directory analysis to worker processes.
 * @param data_source the data source with the directories to analyze
 * @param temp_files the temporary files directory
 * @param notify_fifos the FIFOs on which to read for workers to notify end of tasks
 * @param command_fifos the FIFOs on which to send tasks to workers
 * @param nb_proc the maximum number of simultaneous tasks, = to number of workers
 * Uses @see send_task
 */
void fifo_process_directory(char *data_source, char *temp_files, int *notify_fifos, int *command_fifos, uint16_t nb_proc) {
    DIR *dir;
    struct dirent *entry;
    fd_set read_fds;
    int maxfd;
    int i;
    int completed_tasks = 0;
    // Check the parameters
    if (data_source == NULL || temp_files == NULL || notify_fifos == NULL || command_fifos == NULL || nb_proc == 0) {
        fprintf(stderr, "Invalid parameters\n");
        exit(1);
    }
    // Open the data source directory
    dir = opendir(data_source);
    if (dir == NULL) {
        perror("opendir");
        exit(1);
    }
    // Iterate over the directories in the data source
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            // Skip . and ..
            continue;
        }

        if (completed_tasks < nb_proc) {
            // There are available worker processes, send a task
            send_task(data_source, temp_files, entry->d_name, command_fifos[completed_tasks]);
            completed_tasks++;
        } else {
            // Wait for a worker process to finish its task

            // Initialize the fd_set and get the maximum file descriptor value
            maxfd = prepare_select(&read_fds, notify_fifos, nb_proc);
            // Wait for a notification FIFO to become readable
            if (select(maxfd + 1, &read_fds, NULL, NULL, NULL) < 0) {
                perror("select");
                exit(1);
            }

            // Find the readable FIFO
            for (i = 0; i < nb_proc; i++) {
                if (FD_ISSET(notify_fifos[i], &read_fds)) {
                    // Read the notification from the worker process that just finished its task
                    char notification[1024];
                    if (read(notify_fifos[i], &notification, sizeof(notification)) < 0) {
                        perror("read");
                        exit(1);
                    }

                    // Check the notification message
                    if (strcmp(notification, "Task completed") == 0) {
                        // Send a new task to the worker process that just finished its task
                        send_task(data_source, temp_files, entry->d_name, command_fifos[i]);
                    }
                    break;
                }
            }
        }
    }
    // Cleanup
    closedir(dir);
}

/*!
 * @brief fifo_process_files is the main function to distribute files analysis to worker processes.
 * @param data_source the data source with the files to analyze
 * @param temp_files the temporary files directory (step1_output is here)
 * @param notify_fifos the FIFOs on which to read for workers to notify end of tasks
 * @param command_fifos the FIFOs on which to send tasks to workers
 * @param nb_proc  the maximum number of simultaneous tasks, = to number of workers
 */
void fifo_process_files(char *data_source, char *temp_files, int *notify_fifos, int *command_fifos, uint16_t nb_proc) {
    fd_set read_fds;
    int maxfd;
    int i;
    int completed_tasks = 0;

    // Check the parameters
    if (data_source == NULL || temp_files == NULL || notify_fifos == NULL || command_fifos == NULL || nb_proc == 0) {
        fprintf(stderr, "Invalid parameters\n");
        exit(1);
    }

    char step1_output[255];
    snprintf(step1_output, 255, "%s/%s", temp_files, "step1_output");
    FILE *input_file = fopen(step1_output, "r");
    if (input_file == NULL) return;

    char file[STR_MAX_LEN];


    // Iterate over the files in the step1_output directory
    while (fgets(file, STR_MAX_LEN, input_file) != NULL) {

        file[strlen(file) - 1] = '\0'; // Remove new line character

        if (completed_tasks < nb_proc) {
            // There are available worker processes, send a task
            send_task_file(data_source, temp_files, file, command_fifos[completed_tasks]);
            completed_tasks++;
        } else {
            // Wait for a worker process to finish its task

            // Initialize the fd_set and get the maximum file descriptor value
            maxfd = prepare_select(&read_fds, notify_fifos, nb_proc);
            // Wait for a notification FIFO to become readable
            if (select(maxfd + 1, &read_fds, NULL, NULL, NULL) < 0) {
                perror("select");
                exit(1);
            }

            // Find the readable FIFO
            for (i = 0; i < nb_proc; i++) {
                if (FD_ISSET(notify_fifos[i], &read_fds)) {
                    // Read the notification from the worker process that just finished its task
                    char notification[1024];
                    if (read(notify_fifos[i], &notification, sizeof(notification)) < 0) {
                        perror("read");
                        exit(1);
                    }

                    // Check the notification message
                    if (strcmp(notification, "Task completed") == 0) {
                        // Send a new task to the worker process that just finished its task
                        send_task_file(data_source, temp_files, file, command_fifos[i]);
                    }
                    break;
                }
            }
        }
    }
    // Cleanup
    fclose(input_file);
}