// Author: Keonwoo Oh (koh3@umd.edu)

#ifndef CONFIG_H
#define CONFIG_H

#include <string>
#include <unordered_map>
// TODO currently immutable, so no need for concurrency control, 
// that may change in the future.
//Simple config object for bootstrapping
class Config {
    
    public:
        /* 
         * Reads in file in the provided path and initialize a hash map that 
         * stores config name, value pairs.
         */
        Config(const std::string& config_path);
        ~Config();
        /* 
         * Returns the value of the param with the given name
         */
        std::string getParam(const std::string& param_name) const;
    
    private:
        std::unordered_map<std::string, std::string> config_params_;

};

#endif  // CONFIG_H