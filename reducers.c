//
// Created by flassabe on 26/10/22.
//

#include "reducers.h"

#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

#include "global_defs.h"
#include "utility.h"

/*!
 * @brief add_source_to_list adds an e-mail to the sources list. If the e-mail already exists, do not add it.
 * @param list the list to update
 * @param source_email the e-mail to add as a string
 * @return a pointer to the updated beginning of the list
 */
sender_t *add_source_to_list(sender_t *list, char *source_email) {
    sender_t *current = list;
    while (current != NULL) {
        if (strcmp(current->sender_address, source_email) == 0) {
            // Email already exists in list, return list unchanged
            return list;
        }
        current = current->next;
    }
    // Email not found in list, add it
    sender_t *new_sender = malloc(sizeof(sender_t));
    strcpy(new_sender->sender_address, source_email);
    new_sender->head = NULL;
    new_sender->tail = NULL;
    new_sender->prev = NULL;
    new_sender->next = list;
    if (list != NULL) {
        list->prev = new_sender;
    }
    return new_sender;
}

/*!
 * @brief clear_sources_list clears the list of e-mail sources (therefore clearing the recipients of each source)
 * @param list a pointer to the list to clear
 */
void clear_sources_list(sender_t *list) {
    sender_t *current = list;
    while (current != NULL) {
        recipient_t *recipient = current->head;
        while (recipient != NULL) {
            recipient_t *next = recipient->next;
            free(recipient);
            recipient = next;
        }
        sender_t *next = current->next;
        free(current);
        current = next;
    }
}

/*!
 * @brief find_source_in_list looks for an e-mail address in the sources list and returns a pointer to it.
 * @param list the list to look into for the e-mail
 * @param source_email the e-mail as a string to look for
 * @return a pointer to the matching source, NULL if none exists
 */
sender_t *find_source_in_list(sender_t *list, char *source_email) {
// 1. Check parameters
    if (list == NULL) return NULL;
    if (source_email == NULL) return NULL;

// 2. Iterate over the list
    sender_t *current_source = list;
    while (current_source != NULL) {
        if (strcmp(current_source->sender_address, source_email) == 0) {
            return current_source;
        }
        current_source = current_source->next;
    }

// 3. Return NULL if e-mail not found
    return NULL;
}

/*!
 * @brief add_recipient_to_source adds or updates a recipient in the recipients list of a source. It looks for
 * the e-mail in the recipients list: if it is found, its occurrences is incremented, else a new recipient is created
 * with its occurrences = to 1.
 * @param source a pointer to the source to add/update the recipient to
 * @param recipient_email the recipient e-mail to add/update as a string
 */
void add_recipient_to_source(sender_t *source, char *recipient_email) {
    // 1. Check parameters
    if (source == NULL) return;
    if (recipient_email == NULL) return;
    // 2. Check if e-mail already exists in list
    recipient_t *current_recipient = source->head;
    while (current_recipient != NULL) {
        if (strcmp(current_recipient->recipient_address, recipient_email) == 0) {
            current_recipient->occurrences++;
            return;
        }
        current_recipient = current_recipient->next;
    }
    // 3. If not, add it
    recipient_t *new_recipient = (recipient_t *) malloc(sizeof(recipient_t));
    strcpy(new_recipient->recipient_address, recipient_email);
    new_recipient->occurrences = 1;
    new_recipient->next = NULL;
    new_recipient->prev = source->tail;
    if (source->head == NULL) {
        source->head = new_recipient;
    } else {
        source->tail->next = new_recipient;
    }
    source->tail = new_recipient;
}

/*!
 * @brief files_list_reducer is the first reducer. It uses concatenates all temporary files from the first step into
 * a single file. Don't forget to sync filesystem before leaving the function.
 * @param data_source the data source directory (its directories have the same names as the temp files to concatenate)
 * @param temp_files the temporary files directory, where to read files to be concatenated
 * @param output_file path to the output file (default name is step1_output, but we'll keep it as a parameter).
 */
void files_list_reducer(char *data_source, char *temp_files, char *output_file) {
    // 1. Check parameters
    if (data_source == NULL || temp_files == NULL || output_file == NULL) return;
    // 2. Open output file for writing
    FILE *out = fopen(output_file, "w");
    if (out == NULL) return;
    // 3. Iterate over subdirectories in data_source
    DIR *dir = opendir(data_source);
    if (dir == NULL) return;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) continue;
        // 4. Concatenate files from subdirectory into output file
        char temp_path[STR_MAX_LEN];
        sprintf(temp_path, "%s/%s", temp_files, entry->d_name);
        FILE *temp = fopen(temp_path, "r");
        if (temp == NULL) continue;
        char line[STR_MAX_LEN];
        while (fgets(line, STR_MAX_LEN, temp) != NULL) {
            fputs(line, out);
        }
        fclose(temp);
        remove(temp_path);
    }
    closedir(dir);
    // 5. Close output file and sync filesystem
    fclose(out);
    sync();
}

/*!
 * @brief files_reducer opens the second temporary output file (default step2_output) and collates all sender/recipient
 * information as defined in the project instructions. Stores data in a double level linked list (list of source e-mails
 * containing each a list of recipients with their occurrences).
 * @param temp_file path to temp output file
 * @param output_file final output file to be written by your function
 */
void files_reducer(char *temp_file, char *output_file) {
    // Open the temporary output file for reading
    FILE *temp_fp = fopen(temp_file, "r");
    if (temp_fp == NULL) {
        fprintf(stderr, "Error opening temporary output file\n");
        exit(1);
    }

    // Open the final output file for writing
    FILE *output_fp = fopen(output_file, "w");
    if (output_fp == NULL) {
        fprintf(stderr, "Error opening final output file\n");
        fclose(temp_fp);
        exit(1);
    }

    // Initialize the linked list of sources
    sender_t *senders = NULL;

    // Read each line in the temporary output file
    char line[STR_MAX_LEN];
    while (fgets(line, STR_MAX_LEN, temp_fp) != NULL) {
        // Parse the sender and recipient email addresses from the line
        char *sender = strtok(line, " ");

        sender = str_trim(sender);

        // Add the sender to the linked list if it does not already exist
        senders = add_source_to_list(senders, sender);

        // Find the sender in the linked list
        sender_t *source = find_source_in_list(senders, sender);

        char *recipient = strtok(NULL, " ");

        while (recipient != NULL) {

            // Remove newline character if it is present at the end of the email address
            size_t recipient_len = strlen(recipient);
            if (recipient[recipient_len - 1] == '\n') recipient[recipient_len - 1] = '\0';

            // Add the recipient to the sender's list of recipients
            add_recipient_to_source(source, recipient);

            // Get the next token
            recipient = strtok(NULL, " ");
        }
    }

    // Iterate over the linked list of senders and write the summary to the final output file
    sender_t *current_sender = senders;
    while (current_sender != NULL) {
        // Write the sender email address to the output file
        fprintf(output_fp, "%s ", current_sender->sender_address);

        // Iterate over the sender's list of recipients and write their occurrences to the output file
        recipient_t *current_recipient = current_sender->head;
        while (current_recipient != NULL) {
            fprintf(output_fp, "%u:%s ", current_recipient->occurrences, current_recipient->recipient_address);
            current_recipient = current_recipient->next;
        }

        // Write a newline character to the output file
        fprintf(output_fp, "\n");

        // Move to the next sender in the linked list
        current_sender = current_sender->next;
    }

    // Close the files and free the memory allocated for the linked list
    fclose(temp_fp);
    fclose(output_fp);
    clear_sources_list(senders);
}