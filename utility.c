//
// Created by flassabe on 26/10/22.
//this must compile on linux

#include "utility.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <libgen.h>
#include <unistd.h>
#include <ctype.h>


#include "global_defs.h"

/*!
 * @brief cat_path concatenates two file system paths into a result. It adds the separation /  if required.
 * @param prefix first part of the complete path
 * @param suffix second part of the complete path
 * @param full_path resulting path
 * @return pointer to full_path if operation succeeded, NULL else
 */
char *concat_path(char *prefix, char *suffix, char *full_path) {
    if (prefix == NULL || suffix == NULL || full_path == NULL) return NULL;
    strcpy(full_path, prefix);
    if (full_path[strlen(full_path) - 1] != '/') {
        strcat(full_path, "/");
    }
    strcat(full_path, suffix);
    return full_path;
}

/*!
 * @brief directory_exists tests if directory located at path exists
 * @param path the path whose existence to test
 * @return true if directory exists, false else
 */
bool directory_exists(char *path) {
    if (access(path, F_OK) == 0) {
        return true;
    }
    return false;
}

/*!
 * @brief path_to_file_exists tests if a path leading to a file exists. It separates the path to the file from the
 * file name itself. For instance, path_to_file_exists("/var/log/Xorg.0.log") will test if /var/log exists and is a
 * directory.
 * @param path the path to the file
 * @return true if path to file exists, false else
 */
bool path_to_file_exists(char *path) {
    struct stat sb;
    if(stat(path, &sb) == 0 &&  S_ISREG(sb.st_mode)){
		return true;
	}
    return false;
}

/*!
 * @brief sync_temporary_files waits for filesystem syncing for a path
 * @param temp_dir the path to the directory to wait for
 * Use fsync and dirfd
 */
void sync_temporary_files(char *temp_dir) {
    if (temp_dir == NULL) return;
    DIR *dir = opendir(temp_dir);
    if (dir == NULL) return;
    int dir_fd = dirfd(dir);
    if (dir_fd == -1) return;
    fsync(dir_fd);
    closedir(dir);
}

/*!
 * @brief next_dir returns the next directory entry that is not . or ..
 * @param entry a pointer to the current struct dirent in caller
 * @param dir a pointer to the already opened directory
 * @return a pointer to the next not . or .. directory, NULL if none remain
 */
struct dirent *next_dir(struct dirent *entry, DIR *dir) {
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
            return entry;
        }
    }
    return NULL;
}

char *str_trim(char *str) {
    if (str == NULL) exit(0);
    // Trim leading whitespace
    while (*str != '\0' && isspace(*str)) str++;
    // Trim trailing whitespace
    size_t len = strlen(str);
    while (len > 0 && isspace(str[len - 1])) len--;
    str[len] = '\0';
    return str;
}

void str_remove_char(char *str, char c) {
    if (str == NULL) exit(0);
    char *src, *dst;
    for (src = dst = str; *src != '\0'; src++) {
        *dst = *src;
        if (*dst != c) {
            dst++;
        }
    }
    *dst = '\0';
}