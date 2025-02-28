// Author: Keonwoo Oh (koh3@umd.edu)

#include <fstream>
#include <iostream>
#include <sstream>

#include "common/config.h"

//TODO: probably replace this with xml format etc.
Config::Config(const std::string& config_path) {
    //read in config file
    std::string line;
    std::ifstream config_file(config_path);
    if (config_file.is_open()) {
        config_params_["config_path"] = config_path;
        while (getline(config_file, line)) {
            std::stringstream ss(line);
            std::string config_name, config_val;
            getline(ss, config_name, ' '); 
            getline(ss, config_val);
            config_params_[config_name] = config_val;
        }
        config_file.close();
    }
    else {
        std::cout << "Warning: config file not found" << std::endl;
    }

}

Config::~Config() {

}

std::string Config::getParam(const std::string& param_name) const {
    auto iter = config_params_.find(param_name); 
    if (iter != config_params_.end()) {
        return iter->second; 
    }
    else {
        return "";
    }
}