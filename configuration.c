//
// Created by flassabe on 14/10/22.
//

#include "configuration.h"

#include <getopt.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>

#include "utility.h"

/*!
 * @brief make_configuration makes the configuration from the program parameters. CLI parameters are applied after
 * file parameters. You shall keep two configuration sets: one with the default values updated by file reading (if
 * configuration file is used), a second one with CLI parameters, to overwrite the first one with its values if any.
 * @param base_configuration a pointer to the base configuration to be updated
 * @param argv the main argv
 * @param argc the main argc
 * @return the pointer to the updated base configuration
 */
configuration_t *make_configuration(configuration_t *base_configuration, char *argv[], int argc) {
    // 1. Read CLI parameters
    int opt;
    while ((opt = getopt(argc, argv, "d:o:t:vn")) != -1) {
        switch (opt) {
            case 'd':
		strcpy(base_configuration->data_path,optarg);
                break;
            case 'o':
                strcpy(base_configuration->output_file,optarg);
                break;
	    case 't':
                strcpy(base_configuration->temporary_directory,optarg);
                break;
	    case 'v':
                base_configuration->is_verbose = false;
                break;
	    case 'n':
                base_configuration->process_count = atoi(optarg);
                break;
            default:
                fprintf(stderr, "Usage: %s [-c config_file] [-v]\n", argv[0]);
                exit(EXIT_FAILURE);
        }
    }
    // 2. Read configuration file (already done in the 'c' case)
    // 3. Apply CLI parameters
    // update base_configuration with values from argv
    // 4. Return configuration
    return base_configuration;
}

/*!
 * @brief skip_spaces advances a string pointer to the first non-space character
 * @param str the pointer to advance in a string
 * @return a pointer to the first non-space character in str
 */
char *skip_spaces(char *str) {
    if (str == NULL){
        printf("error skipping spaces on string");
        exit(0);
    }else{
        while (isspace(*str)) {
            str++;
        }
        return str;
    }

}

/*!
 * @brief check_equal looks for an optional sequence of spaces, an equal '=' sign, then a second optional sequence
 * of spaces
 * @param str the string to analyze
 * @return a pointer to the first non-space character after the =, NULL if no equal was found
 */
char *check_equal(char *str) {
    if (str == NULL){
        printf("Error analyzing = from string");
        exit(0);
    }else{
        str = skip_spaces(str);
        if (*str == '=') {
            str++;
            str = skip_spaces(str);
            return str;
        } else {
            return NULL;
        }
    }

}

/*!
 * @brief get_word extracts a word (a sequence of non-space characters) from the source
 * @param source the source string, where to find the word
 * @param target the target string, where to copy the word
 * @return a pointer to the character after the end of the extracted word
 */
char *get_word(char *source, char *target) {
    source = skip_spaces(source);
    while (!isspace(*source) && *source != '\0') {
        *target = *source;
        target++;
        source++;
    }
    *target = '\0';
    return source;
}

/*!
 * @brief read_cfg_file reads a configuration file (with key = value lines) and extracts all key/values for
 * configuring the program (data_path, output_file, temporary_directory, is_verbose, cpu_core_multiplier)
 * @param base_configuration a pointer to the configuration to update and return
 * @param path_to_cfg_file the path to the configuration file
 * @return a pointer to the base configuration after update, NULL is reading failed.
 */
configuration_t *read_cfg_file(configuration_t *base_configuration, char *path_to_cfg_file) {
    FILE *cfg_file = fopen(path_to_cfg_file, "r");
    if (cfg_file == NULL) {
        return NULL;
    }
    char line[STR_MAX_LEN];
    char key[STR_MAX_LEN];
    char value[STR_MAX_LEN];
    while (fgets(line, STR_MAX_LEN, cfg_file) != NULL) {
        char *line_ptr = line;
        line_ptr = get_word(line_ptr, key);
        line_ptr = check_equal(line_ptr);
        if (line_ptr != NULL) {
            line_ptr = get_word(line_ptr, value);
            if (strcmp(key, "data_path") == 0) {
                strcpy(base_configuration->data_path, value);
            } else if (strcmp(key, "temporary_directory") == 0) {
                strcpy(base_configuration->temporary_directory, value);
            } else if (strcmp(key, "output_file") == 0) {
                strcpy(base_configuration->output_file, value);
            } else if (strcmp(key, "is_verbose") == 0) {
                base_configuration->is_verbose = (strcmp(value, "true") == 0);
            } else if (strcmp(key, "cpu_core_multiplier") == 0) {
                base_configuration->cpu_core_multiplier = atoi(value);
            } else if (strcmp(key, "process_count") == 0) {
                base_configuration->process_count = atoi(value);
            }
        }
    }
    fclose(cfg_file);
    return base_configuration;
}

/*!
 * @brief display_configuration displays the content of a configuration
 * @param configuration a pointer to the configuration to print
 */
void display_configuration(configuration_t *configuration) {
    printf("Current configuration:\n");
    printf("\tData source: %s\n", configuration->data_path);
    printf("\tTemporary directory: %s\n", configuration->temporary_directory);
    printf("\tOutput file: %s\n", configuration->output_file);
    printf("\tVerbose mode is %s\n", configuration->is_verbose?"on":"off");
    printf("\tCPU multiplier is %d\n", configuration->cpu_core_multiplier);
    printf("\tProcess count is %d\n", configuration->process_count);
    printf("End configuration\n");
}

/*!
 * @brief is_configuration_valid tests a configuration to check if it is executable (i.e. data directory and temporary
 * directory both exist, and path to output file exists @see directory_exists and path_to_file_exists in utility.c)
 * @param configuration the configuration to be tested
 * @return true if configuration is valid, false else
 */
bool is_configuration_valid(configuration_t *configuration) {
	/*return true;*/

    return directory_exists(configuration->data_path) && directory_exists(configuration->temporary_directory) &&
           path_to_file_exists(configuration->output_file);

}
