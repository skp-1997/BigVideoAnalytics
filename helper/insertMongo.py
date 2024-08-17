import pymongo
import argparse

def flush_database():
    with pymongo.MongoClient('mongodb://127.0.0.1:27017') as client:
        db = client['bigvideo_analytics']
        cameraDetails = db['cameraDetails']
        cameraDetails.delete_many({})
        print('[Mongo] Flushed all the data...')

def insert_camDetails(cameraName, cameraPath):
    cam_dict = {
        'cameraName': cameraName,
        'cameraPath': cameraPath,
        'is_Activated': True
    }
    with pymongo.MongoClient('mongodb://127.0.0.1:27017') as client:
        db = client['bigvideo_analytics']
        cameraDetails = db['cameraDetails']
        cameraDetails.insert_one(cam_dict)
        print(f'[Mongo] Inserted {cam_dict}')

def deactivate_camera(cameraName):
    with pymongo.MongoClient('mongodb://127.0.0.1:27017') as client:
        db = client['bigvideo_analytics']
        cameraDetails = db['cameraDetails']
        result = cameraDetails.update_one(
            {'cameraName': cameraName},
            {"$set": {"is_Activated": False}}
        )
        if result.modified_count > 0:
            print(f'[Mongo] Deactivated {cameraName}')
        else:
            print(f'[Mongo] Camera {cameraName} not found or already deactivated.')

def activate_camera(cameraName):
    with pymongo.MongoClient('mongodb://127.0.0.1:27017') as client:
        db = client['bigvideo_analytics']
        cameraDetails = db['cameraDetails']
        result = cameraDetails.update_one(
            {'cameraName': cameraName},
            {"$set": {"is_Activated": True}}
        )
        if result.modified_count > 0:
            print(f'[Mongo] Activated {cameraName}')
        else:
            print(f'[Mongo] Camera {cameraName} not found or already activated.')

def main():
    parser = argparse.ArgumentParser(description='Call a specific function.')
    parser.add_argument('function', choices=['flush_database', 'insert_camDetails', 'deactivate_camera', 'activate_camera'], help='Function to call')
    parser.add_argument('args', nargs='*', help='Arguments for the function')

    args = parser.parse_args()

    # Dynamically call the function based on the command-line argument
    if args.function == 'flush_database':
        flush_database()
    elif args.function == 'insert_camDetails':
        if len(args.args) != 2:
            print('Error: insert_camDetails requires exactly 2 arguments.')
        else:
            insert_camDetails(args.args[0], args.args[1])
    elif args.function == 'deactivate_camera':
        if len(args.args) != 1:
            print('Error: deactivate_camera requires exactly 1 argument.')
        else:
            deactivate_camera(args.args[0])
    elif args.function == 'activate_camera':
        if len(args.args) != 1:
            print('Error: activate_camera requires exactly 1 argument.')
        else:
            activate_camera(args.args[0])
    else:
        print('Not able to call...')

if __name__ == '__main__':
    main()
