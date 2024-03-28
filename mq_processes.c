//
// Created by flassabe on 10/11/22.
//

#include "mq_processes.h"

#include <sys/msg.h>

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <bits/types/sig_atomic_t.h>
#include <stdio.h>

#include "utility.h"
#include "analysis.h"

/*!
 * @brief make_message_queue creates the message queue used for communications between parent and worker processes
 * @return the file descriptor of the message queue
 */
int make_message_queue() {
    return msgget(IPC_PRIVATE, 0666 | IPC_CREAT);
}

/*!
 * @brief close_message_queue closes a message queue
 * @param mq the descriptor of the MQ to close
 */
void close_message_queue(int mq) {
    msgctl(mq, IPC_RMID, NULL);
}

/*!
 * @brief child_process is the function handling code for a child
 * @param mq message queue descriptor used to communicate with the parent
 */
void child_process(int mq) {
// 1. Endless loop (interrupted by a task whose callback is NULL)
    while (1) {
// 2. Upon reception of a task: check is not NULL
        mq_message_t message;
        if (msgrcv(mq, &message, sizeof(task_t), 0, 0) == -1) {
            perror("msgrcv");
            return;
        }
        task_t *task = (task_t *) message.mtext;
// 2 bis. If not NULL -> execute it and notify parent
        if (task != NULL) {
            task->task_callback(task);
            message.mtype = 1;
            if (msgsnd(mq, &message, sizeof(task_t), 0) == -1) {
                perror("msgsnd");
                return;
            }
        }
// 2 ter. If NULL -> leave loop
        else {
            break;
        }
    }
// 3. Cleanup
}

/*!
 * @brief mq_make_processes makes a processes pool used for tasks execution
 * @param config a pointer to the program configuration (with all parameters, inc. processes count)
 * @param mq the identifier of the message queue used to communicate between parent and children (workers)
 * @return a malloc'ed array with all children PIDs
 */
pid_t *mq_make_processes(configuration_t *config, int mq) {
// 1. Create PIDs array
    pid_t *children = malloc(sizeof(pid_t) * config->process_count);
    if (children == NULL) {
        perror("malloc error");
        return NULL;
    }

// 2. Loop over process_count to fork
    for (int i = 0; i < config->process_count; i++) {
        pid_t child_pid = fork();
        if (child_pid == 0) {
// 2 bis. in fork child part, start listening on message queue
            child_process(mq);
            exit(0);
        } else if (child_pid > 0) {
// parent process
            children[i] = child_pid;
        } else {
// fork error
            perror("fork error");
            free(children);
            return NULL;
        }
    }

// 3. Cleanup
    return children;
}

/*!
 * @brief close_processes commands all workers to terminate
 * @param config a pointer to the configuration
 * @param mq the message queue to communicate with the workers
 * @param children the array of children's PIDs
 */
void close_processes(configuration_t *config, int mq, pid_t children[]) {
    // 1. Loop over process_count to send a task with NULL callback
    for (int i = 0; i < config->process_count; i++) {
        task_t task = { .task_callback = NULL };
        mq_message_t message;
        message.mtype = children[i];
        memcpy(message.mtext, &task, sizeof(task_t));
        msgsnd(mq, &message, sizeof(task_t), 0);
    }

    // 2. Loop over process_count to wait for children
    for (int i = 0; i < config->process_count; i++) {
        wait(NULL);
    }

    // 3. Cleanup
}

/*!
 * @brief send_task_to_mq sends a directory task to a worker through the message queue. Directory task's object is
 * data_source/target_dir, temp output file is temp_files/target_dir. Task is sent through MQ with topic equal to
 * the worker's PID
 * @param data_source the data source directory
 * @param temp_files the temporary files directory
 * @param target_dir the name of the target directory
 * @param mq the MQ descriptor
 * @param worker_pid the worker PID
 */
void send_task_to_mq(char data_source[], char temp_files[], char target_dir[], int mq, pid_t worker_pid) {
// 1. Create task
        directory_task_t dir_task;
        strcpy(dir_task.object_directory, data_source);
        strcpy(dir_task.temporary_directory, temp_files);
        strcpy(dir_task.object_directory, target_dir);
        dir_task.task_callback = parse_dir;

// 2. Create message
        mq_message_t message;
        message.mtype = worker_pid;
        memcpy(message.mtext, &dir_task, sizeof(dir_task));

// 3. Send message
        msgsnd(mq, &message, sizeof(dir_task), 0);
}

/*!
 * @brief send_file_task_to_mq sends a file task to a worker. It operates similarly to @see send_task_to_mq
 * @param data_source the data source directory
 * @param temp_files the temporary files directory
 * @param target_file the target filename
 * @param mq the MQ descriptor
 * @param worker_pid the worker's PID
 */
void send_file_task_to_mq(char data_source[], char temp_files[], char target_file[], int mq, pid_t worker_pid) {
// 1. Create task
        file_task_t file_task;
        file_task.task_callback = process_file;
        strcpy(file_task.object_file, data_source);
        strcpy(file_task.temporary_directory, temp_files);
        strcpy(file_task.object_file, target_file);

// 2. Create message
        mq_message_t message;
        message.mtype = worker_pid;
        memcpy(message.mtext, &file_task, sizeof(file_task_t));

// 3. Send message
        if (msgsnd(mq, &message, sizeof(file_task_t), 0) == -1) {
            perror("Error sending message");
        }

}

/*!
 * @brief mq_process_directory root function for parallelizing directory analysis over workers. Must keep track of the
 * tasks count to ensure every worker handles one and only one task. Relies on two steps: one to fill all workers with
 * a task each, then, waiting for a child to finish its task before sending a new one.
 * @param config a pointer to the configuration with all relevant path and values
 * @param mq the MQ descriptor
 * @param children the children's PIDs used as MQ topics number
 */
void mq_process_directory(configuration_t *config, int mq, pid_t children[]) {
// 1. Check parameters
    if (config == NULL || mq < 0 || children == NULL) return;

// 2. Iterate over children and provide one directory to each
    int running_workers = config->process_count;
    DIR * d = opendir(config->data_path);
    if (d == NULL) return;
    struct dirent * dir;
    while ((dir = readdir(d)) != NULL) {
        if (dir->d_type == DT_DIR && strcmp(dir->d_name, ".") != 0 && strcmp(dir->d_name, "..") != 0) {
            send_task_to_mq(config->data_path, config->temporary_directory, dir->d_name, mq, children[config->process_count - running_workers]);
            running_workers--;
        }
    }
    closedir(d);

// 3. Loop while there are directories to process, and while all workers are processing
    mq_message_t message;
    int tasks_left = running_workers;
    while (tasks_left > 0) {
        msgrcv(mq, &message, sizeof(task_t), 0, 0);
        running_workers--;
        tasks_left--;
        if (running_workers > 0) {
            send_task_to_mq(config->data_path, config->temporary_directory, dir->d_name, mq, children[config->process_count - running_workers]);
            running_workers--;
            tasks_left--;
        }
    }

// 4. Cleanup
    while (running_workers > 0) {
        msgrcv(mq, &message, sizeof(task_t), 0, 0);
        running_workers--;
    }
}

/*!
 * @brief mq_process_files root function for parallelizing files analysis over workers. Operates as
 * @see mq_process_directory to limit tasks to one on each worker.
 * @param config a pointer to the configuration with all relevant path and values
 * @param mq the MQ descriptor
 * @param children the children's PIDs used as MQ topics number
 */
void mq_process_files(configuration_t *config, int mq, pid_t children[]) {
    // 1. Check parameters
    if (config == NULL || mq < 0 || children == NULL) return;

    // 2. Iterate over children and provide one file to each
    int running_workers = config->process_count;
    FILE *files_list = fopen("step1_output", "r");
    if (files_list == NULL) return;
    char file_path[STR_MAX_LEN];
    while (fgets(file_path, STR_MAX_LEN, files_list) != NULL) {
        // remove newline character from file path
        file_path[strcspn(file_path, "\n")] = 0;
        send_file_task_to_mq(config->data_path, config->temporary_directory, file_path, mq, children[config->process_count - running_workers]);
        running_workers--;
    }
    fclose(files_list);

    // 3. Loop while there are files to process, and while all workers are processing
    mq_message_t message;
    int tasks_left = running_workers;
    while (tasks_left > 0) {
        msgrcv(mq, &message, sizeof(message), 0, 0);
        running_workers++;
        tasks_left--;
        if (running_workers < config->process_count) {
            send_file_task_to_mq(config->data_path, config->temporary_directory, file_path, mq, children[config->process_count - running_workers]);
            running_workers--;
            tasks_left++;
        }
    }

    // 4. Cleanup
    while (running_workers > 0) {
        msgrcv(mq, &message, sizeof(message), 0, 0);
        running_workers--;
    }

}
