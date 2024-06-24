

export const SystemConfig = {

    getThresholds: () => {
       return Promise.resolve({"processing": {
        'avgProcessingSpeedInSec': 50171, 
        'validationFailuresCount': 10,
        'dedupFailuresCount': 10,
        'denormFailureCount': 10,
        'transformFailureCount': 10
       }})
    }

}