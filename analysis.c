//
// Created by flassabe on 26/10/22.
//

#include "analysis.h"

#include <dirent.h>
#include <stddef.h>
#include <unistd.h>
#include <libgen.h>
#include <string.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/file.h>

#include "utility.h"

/*!
 * @brief parse_dir parses a directory to find all files in it and its subdirs (recursive analysis of root directory)
 * All files must be output with their full path into the output file.
 * @param path the path to the object directory
 * @param output_file a pointer to an already opened file
 */
void parse_dir(char *path, FILE *output_file) {
    // 1. Check parameters
    if(path == NULL || output_file == NULL) return;
    if(!directory_exists(path)) return;
    // if(!path_to_file_exists(output_file)) return;

    // 2. Gor through all entries: if file, write it to the output file; if a dir, call parse dir on it
    DIR * d = opendir(path); // open the path
    if(d==NULL) return; // if was not able, return
    struct dirent * dir; // for the directory entries
    while ((dir = readdir(d)) != NULL) // if we were able to read somehting from the directory
    {
        if(dir-> d_type != DT_DIR)
            fprintf(output_file,"%s/%s\n",path ,dir->d_name);
        else
        if(dir -> d_type == DT_DIR && strcmp(dir->d_name,".")!=0 && strcmp(dir->d_name,"..")!=0 ) // if it is a directory
        {
            char new_path[255];
            parse_dir(concat_path(path, dir -> d_name, new_path), output_file);
        }
    }
    // 3. Clear all allocated resources
    closedir(d); // finally close the directory
}

/*!
 * @brief clear_recipient_list clears all recipients in a recipients list
 * @param list the list to be cleared
 */
void clear_recipient_list(simple_recipient_t *list) {
    simple_recipient_t *cur = list;
    while (cur != NULL) {
        simple_recipient_t *next = cur->next;
        free(cur);
        cur = next;
    }
    list = NULL;
}

/*!
 * @brief add_recipient_to_list adds a recipient to a recipients list (as a pointer to a recipient)
 * @param recipient_email the string containing the e-mail to add
 * @param list the list to add the e-mail to
 * @return a pointer to the new recipient (to update the list with)
 */
simple_recipient_t *add_recipient_to_list(char *recipient_email, simple_recipient_t *list) {
    simple_recipient_t *new_recipient = (simple_recipient_t *) malloc(sizeof(simple_recipient_t));
    strcpy(new_recipient->email, recipient_email);
    new_recipient->next = NULL;

    if (list == NULL) {
        return new_recipient;
    }

    simple_recipient_t *cur = list;
    while (cur->next != NULL) {
        cur = cur->next;
    }
    cur->next = new_recipient;
    return list;
}



/*!
 * @brief extract_emails extracts all the e-mails from a buffer and put the e-mails into a recipients list
 * @param buffer the buffer containing one or more e-mails
 * @param list the resulting list
 * @return the updated list
 */
simple_recipient_t *extract_emails(char *buffer, simple_recipient_t *list) {
    if (buffer == NULL) return list; // Check parameters
    char *email = strtok(buffer, " ");
    while (email != NULL) {
        // Trim leading/trailing spaces and newlines
        email = str_trim(email);
        str_remove_char(email,',');
        // Add email to list
        list = add_recipient_to_list(email, list);
        email = strtok(NULL, " ");
    }

    return list;
}

/*!
 * @brief extract_e_mail extracts an e-mail from a buffer
 * @param buffer the buffer containing the e-mail
 * @param destination the buffer into which the e-mail is copied
 */
void extract_e_mail(char *buffer, char *destination) {
    if (buffer == NULL || destination == NULL) return; // Check parameters
    // Extract email address from buffer
    char *email = strtok(buffer, " ");
    // Trim leading and trailing whitespace from email
    email = str_trim(email);
    // Remove newline character if it is present at the end of the email address
    size_t email_len = strlen(email);
    if (email[email_len - 1] == '\n') email[email_len - 1] = '\0';
    // Copy email address to destination
    strcpy(destination, email);
}


// Used to track status in e-mail (for multi lines To, Cc, and Bcc fields)
typedef enum {IN_DEST_FIELD, OUT_OF_DEST_FIELD} read_status_t;

/*!
 * @brief parse_file parses mail file at filepath location and writes the result to
 * file whose location is on path output
 * @param filepath name of the e-mail file to analyze
 * @param output path to output file
 * Uses previous utility functions: extract_email, extract_emails, add_recipient_to_list,
 * and clear_recipient_list
 */
void parse_file(char *filepath, char *output) {
    // 1. Check parameters
    if (filepath == NULL || output == NULL) return;
    // 2. Go through e-mail and extract From: address into a buffer
    char from_email[STR_MAX_LEN];
    simple_recipient_t *recipients = NULL;
    // Open email file
    FILE *email_file = fopen(filepath, "r");
    if (email_file == NULL) return;
    // Open output file
    FILE *output_file = fopen(output, "a");
    if (output_file == NULL) return;
    char buffer[STR_MAX_LEN];

    bool from_extracted = false;
    bool to_extracted = false;
    bool cc_extracted = false;
    bool bcc_extracted = false;
    while (fgets(buffer, STR_MAX_LEN, email_file) != NULL) {
        if (strncmp(buffer, "From:", 5) == 0) {
            if (!from_extracted) {
                // Extract sender's email address
                extract_e_mail(buffer + 6, from_email);
                from_extracted = true;
            }
        }else if (strncmp(buffer, "To:", 3) == 0) {
            if (!to_extracted){
                // Extract recipients' email addresses from the "To:" field
                recipients = extract_emails(buffer + 4, recipients);
                to_extracted = true;
            }
        }else if (strncmp(buffer, "Cc:", 3) == 0) {
            if (!cc_extracted){
                // Extract recipients' email addresses from the "Cc:" field
                recipients = extract_emails(buffer + 4, recipients);
                cc_extracted = true;
            }
        }else if (strncmp(buffer, "Bcc:", 4) == 0) {
            if (!bcc_extracted){
                // Extract recipients' email addresses from the "Bcc:" field
                recipients = extract_emails(buffer + 5, recipients);
                bcc_extracted = true;
            }
        }else if (strncmp(buffer, "X-From:", 7) == 0){
            break;
        }
    }
    // 4. Lock output file
    flock(fileno(output_file), LOCK_EX);
    // 5. Write to output file according to project instructions
    fprintf(output_file, "%s", from_email);
    simple_recipient_t *current = recipients;
    while (current != NULL) {
        fprintf(output_file, " %s", current->email);
        current = current->next;
    }
    fprintf(output_file, "\n");

    // 6. Unlock file
    flock(fileno(output_file), LOCK_UN);
    // 7. Close file
    fclose(email_file);
    fclose(output_file);
    // 8. Clear all allocated resources
    clear_recipient_list(recipients);
}



/*!
 * @brief process_directory goes recursively into directory pointed by its task parameter object_directory
 * and lists all of its files (with complete path) into the file defined by task parameter temporary_directory/name of
 * object directory
 * @param task the task to execute: it is a directory_task_t that shall be cast from task pointer
 * Use parse_dir.
 */
void process_directory(task_t *task) {
        // 1. Check parameters
        if (task == NULL) {
            return;
        }
        directory_task_t *dir_task = (directory_task_t *) task;
        if (dir_task->object_directory == NULL || dir_task->temporary_directory == NULL) {
            return;
        }
        // 2. Go through dir tree and find all regular files
        char output_path[255];
        snprintf(output_path, 255, "%s/%s", dir_task->temporary_directory, basename(dir_task->object_directory));
        FILE *output_file = fopen(output_path, "w");
        if (output_file == NULL) {
            return;
        }

        // 3. Write all file names into output file
        parse_dir(dir_task->object_directory, output_file);

        // 4. Clear all allocated resources
        fclose(output_file);
}


/*!
 * @brief process_file processes one e-mail file.
 * @param task a file_task_t as a pointer to a task (you shall cast it to the proper type)
 * Uses parse_file
 */
void process_file(task_t *task) {
    // Check parameters
    if (task == NULL) return;
    file_task_t *file_task = (file_task_t *) task;
    if (file_task == NULL) return;
    // 2. Build full path to all parameters
    char filepath[STR_MAX_LEN];
    strncpy(filepath, file_task->object_file, STR_MAX_LEN);
    char output[STR_MAX_LEN];
    if (concat_path(file_task->temporary_directory, "step2_output", output) == NULL) return;

    // 3. Call parse_file
    parse_file(filepath, output);
}





