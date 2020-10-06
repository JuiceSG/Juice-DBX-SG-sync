# -*- coding: utf-8 -*-
import os
import dropbox
import shotgun_api3
import xml.etree.ElementTree as et
import shutil
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('C:/Juice_Pipeline/config.env')
dbx_token = os.getenv('DBX_TOKEN')
dbx = dropbox.Dropbox(dbx_token)    # instancja Dropboxa
dbx_projects_location = os.getenv('DBX_PROJECTS_LOCATION')    # sciezka pod ktora znajduja sie
sg_token = os.getenv('SG_DROPBOX_SYNC_TOKEN')
sg_script_name = os.getenv('SG_DROPBOX_SYNC_NAME')
sg_address = os.getenv('SG_ADDRESS')
sg = shotgun_api3.Shotgun(sg_address, script_name=sg_script_name, api_key=sg_token)     # instancja polaczenia do SG
sg_project_location = os.getenv('SG_PROJECT_LOCATION')
sg_project_mapped_loc = os.getenv('SG_PROJECT_MAPPED_LOC')
folders_to_skip = os.getenv('FOLDERS_TO_SKIP')
folders_to_skip = folders_to_skip.split(',')
folders_to_skip = map(lambda x: x.lower(), folders_to_skip)
folders_to_skip = list(folders_to_skip)


class FileData:

    """
    Klasa odpowiadajaca za przechowywanie danych na temat plikow tworoznych w czasie synchronizacji.
    Sa one wykorzystywane w czasie tworznie wpisow w SG. Przechowuje także dane na temat czy plik ten
    zostal usuniety badz jest folderem
    """

    def __init__(self, name, local_path, dbx_path, rev, is_deleted, is_folder):
        self.__name = name
        self.__step_name = local_path.split('/')[6]
        self.__local_path = local_path
        self.__dbx_path = dbx_path
        self.__rev = rev
        self.__is_deleted = is_deleted
        self.__is_folder = is_folder

    @property
    def name(self):
        return self.__name

    @property
    def step_name(self):
        return self.__step_name

    @property
    def local_path(self):
        return self.__local_path

    @local_path.setter
    def local_path(self, path):
        self.__local_path = path

    @property
    def dbx_path(self):
        return self.__dbx_path

    @property
    def rev(self):
        return self.__rev

    @property
    def is_deleted(self):
        return self.__is_deleted

    @property
    def is_folder(self):
        return self.__is_folder


class ProjectsStatus:

    """
    Klasa zawierajaca statusy wszystkich projektow, posiada atrybuty zawierajace wszystkie projekty jak i jedynie
    aktywne projekty. Klasa zawiera metode do aktualziajci projektow. Dane przechowuje w pliku .xml dzieki czemu
    moze kontynuowac prace od ostatniego zapytania w razie gdyby skrypt przestal dzialac
    """

    def __init__(self, status):
        self.__projects = []   # zawiera dane aktywnych projektow wraz z kursorami
        self.__projects_fields = ['name', 'sg_status']
        self.__acceptable_status = status
        self.__projects_filters = [
            ['sg_status', 'is', self.__acceptable_status]
            ]
        self.__last_session = '%s_last_session.xml' % status
        if os.path.isfile(self.__last_session):
            self.__load_last_session()
        else:
            self.__projects = self.__get_projects()

    @property
    def projects(self):
        return self.__projects

    def update(self):
        updated_projects = self.__get_projects()
        updated_projects_id = list(map(lambda x: x['id'], updated_projects))
        projects_id = list(map(lambda x: x['id'], self.__projects))
        for project in updated_projects:
            if project['id'] not in projects_id:
                self.__projects.append(project)
        for project in self.__projects:
            if project['id'] not in updated_projects_id:
                self.__projects.remove(project)
        self.__save_last_session()
        print(self.__projects)

    def __load_last_session(self):   # funkcja odzyskujaca dane z poprzedniej sesji na podstawie pliku xml
        tree = et.parse(self.__last_session)
        root = tree.getroot()
        for project in root:
            data_project = {'type': 'Project'}
            for data in project:
                data_project[data.tag] = data.text
            data_project['id'] = int(data_project['id'])
            self.__projects.append(data_project)
        print('Last session data have been loaded')
        print(self.__projects)
        return True

    def __save_last_session(self):  # metoda pozwalajaca na zapis obecnych kluczy do pliku
        root = et.Element('root')
        for project in self.__projects:
            project_data = et.Element('project')
            root.append(project_data)
            et.SubElement(project_data, 'id', name='project_id').text = str(project['id'])
            et.SubElement(project_data, 'name', name='Project_name').text = str(project['name'])
            et.SubElement(project_data, 'sg_status', name='Project_status').text = str(project['sg_status'])
            if 'cursor' in project:
                et.SubElement(project_data, 'cursor', name='last_cursor').text = project['cursor']
        tree = et.ElementTree(root)
        tree.write(self.__last_session)
        return True

    def __get_projects(self):
        print("%s projects" % self.__acceptable_status)
        projects = sg.find('Project', self.__projects_filters, self.__projects_fields)
        print(projects)
        print(len(projects))
        return projects


class SyncDropbox:

    """
    Klasa kopiujaca dane z Dropboxa na dysk lokalny przy uzyciu listy aktywnych projektow, zwraca liste zgranych
    z Dropboxa plikow w postaci slownika zawierajacego projekty oraz listy odpowiadajacych im plikow.
    """

    def __init__(self, projects):
        self.__projects = projects

    def check_dbx_files(self):  # sprawdza zmiany w plikach Dropbox, zwraca liste nowych plikow do sync
        dbx_new_files = []
        for project in self.__projects:
            project_path = '%s%s' % (dbx_projects_location, str(project["name"]))
            if 'cursor' not in project:  # na podstawie cursora bedzie mogl stwierdzic czy zmiany sa w strukturze
                files_to_sync = self.__get_cursor_and_files_list(project, project_path)
            else:
                files_to_sync = self.__get_new_cursor_and_files_list(project)
            if files_to_sync:
                new_files = {'project': project, 'new_files': files_to_sync}
                dbx_new_files.append(new_files)
        print('in check_dbx_file new files %s' % dbx_new_files)
        return dbx_new_files

    def copy_files_from_dbx(self, dbx_new_files):
        if not dbx_new_files:
            return False
        for project in dbx_new_files:
            project_new_files = project['new_files']
            for file in project_new_files:
                if file.is_deleted:
                    print('File was deleted: %s' % file.name)
                    pass
                    # self.__delete_file(file)
                    # print('File was deleted: %s, %s, %s' % (file.local_path, file.dbx_path, file.rev))
                else:
                    self.__is_folder_exist(file.local_path)
                    try:
                        dbx.files_download_to_file(file.local_path, file.dbx_path, file.rev)
                        print('Coping file: %s, %s, %s' % (file.local_path, file.dbx_path, file.rev))
                    except:
                        print('ERROR: unable to download %s' % file.name)
        return True

    def __get_new_cursor_and_files_list(self, project):
        new_files = dbx.files_list_folder_continue(project['cursor'])
        project['cursor'] = new_files.cursor
        new_files = self.__get_file_data(new_files.entries)
        return new_files

    def __get_cursor_and_files_list(self, project, project_path):
        try:  # probuje uzyskac kursor dla prpjektu
            list_folder = dbx.files_list_folder(project_path, recursive=True)
            new_files = self.__get_file_data(list_folder.entries)
            project['cursor'] = list_folder.cursor
        except:
            print("WARNING: Project %s doesn't have folder structure on Dropbox" % (project['name']))
            new_files = []
        return new_files

    def __get_file_data(self, files):   # pobiera dane o pliku niezbedne przy zgrywaniu dodawaniu wpisu w SG itp
        files_list = []
        for file in files:
            name = file.name
            local_path = self.__get_local_path(file)
            if self.__is_folder_to_skip(local_path):
                pass
            dbx_path = file.path_lower
            rev = None
            is_deleted = False
            is_folder = False
            if type(file) is dropbox.files.DeletedMetadata:
                is_deleted = True
            if type(file) is dropbox.files.FileMetadata:
                rev = file.rev
            if type(file) is dropbox.files.FolderMetadata:
                is_folder = True
            file_data = FileData(name, local_path, dbx_path, rev, is_deleted, is_folder)
            files_list.append(file_data)
        return files_list

    @staticmethod
    def __is_folder_to_skip(path):
        folder = path.split('/')[6]
        folder = folder.lower()
        if folder in folders_to_skip:
            return True
        else:
            return False

    @staticmethod
    def __delete_file(file):
        path = file.local_path
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            if not os.listdir(path):
                os.rmdir(path)
            else:
                shutil.rmtree(path)
        else:
            return False
        return True

    @staticmethod
    def __get_local_path(file):
        local_path = file.path_lower.split('/')
        project_index = local_path.index('projects')
        project_name = local_path[project_index + 1]
        path_relative = '/'.join(local_path[(project_index + 2):])
        local_path = '%s%s/prod/%s' % (sg_project_location, project_name, path_relative)
        return local_path

    @staticmethod
    def __is_folder_exist(local_path):
        local_path = local_path.split('/')
        local_path = '/'.join(local_path[:-1])
        if not os.path.exists(local_path):
            os.mkdir(local_path)
        return True


class SyncSG:

    """
    Klasa odpowiedzialna za tworzenie nowych wpisow w SG na podstawie podanej listy zwrotnej z klasy synchronizujacej.
    Przechowuje dane potrzebne do stworzenia projektow w postaci slownika z projektem oraz lista
    plikow. Funkcja odpowiadajaca za stworzenie i usowanie  wpisow zwraca ich liste
    """

    def __init__(self, data_to_sync):
        self.__data_to_sync = data_to_sync

    def create_entities(self):     # metoda tworzaca wpis w sg ze zgranymi plikami
        if not self.__data_to_sync:
            return None
        for data in self.__data_to_sync:
            project = data['project']
            project_new_files = data['new_files']
            if project['sg_status'] == 'Active':
                entities = self.__create_entities_for_active(project, project_new_files)
            if project['sg_status'] == 'Pitch':
                entities = self.__create_entities_for_pitch(project, project_new_files)
        return entities

    def __create_entities_for_active(self, project, project_new_files):
        created_entities = []
        deleted_entities = []
        for file in project_new_files:
            skip = self.__is_folder_to_skip(file.local_path)
            if skip:
                print('Entity is skipped from creating entities: %s' % file.name)
                pass
            if file.is_deleted:
                print("Entity was deleted: %s" % file.name)
                pass
                # del_entities = self.__delete_entity(file, project)
                # deleted_entities.append(del_entities)
            if os.path.isfile(file.local_path):
                new_entity = self.__create_entity(file, project)
                created_entities.append(new_entity)
        entities = {'deleted': deleted_entities, 'created': created_entities}
        return entities

    def __create_entities_for_pitch(self, project, project_new_files):
        created_entities = []
        deleted_entities = []
        for file in project_new_files:
            skip = self.__is_folder_to_skip(file.local_path)
            if skip:
                print('Entity is skipped from creating entities: %s' % file.name)
                pass
            if file.is_deleted:
                print("Entity was deleted: %s" % file.name)
                pass
                # del_entities = self.__delete_entity(file, project)
                # deleted_entities.append(del_entities)
            tmp_path = os.getenv('TMP_PATH')
            file_path = tmp_path + file.name
            print(file.name)
            print(file.dbx_path)
            print(file.rev)
            if not file.is_folder:
                dbx.files_download_to_file(file_path, file.dbx_path, file.rev)
                file.local_path = file_path
                new_entity = self.__create_entity(file, project)
                created_entities.append(new_entity)
        self.__empty_tmp(tmp_path)
        entities = {'deleted': deleted_entities, 'created': created_entities}
        return entities

    def __create_entity(self, file, project):
        file_name = file.name
        local_path = file.local_path
        step_name = file.step_name
        prod_file_name = 'Dropbox_%s' % step_name  # nazwa dla wystapienia w Prod Files
        task_name = 'Dropbox_%s_task' % step_name  # nazwa dla tasku
        sg_step = self.__get_step(step_name)
        prod_file = self.__create_prod_file(project, prod_file_name, step_name)
        task = self.__create_task(project, prod_file, task_name, sg_step)
        version = self.__create_version(project, prod_file, task, file_name, local_path)
        entity = {'prod_file': prod_file, 'task': task, 'version': version}
        sg.upload('Version', version['id'], local_path, field_name='sg_uploaded_movie', display_name=file_name)
        return entity

    def __delete_entity(self, file, project):
        deleted_entities = {}
        project = sg.find_one('Project', [['id', 'is', project['id']]])
        file_name = file.name
        local_path = file.local_path
        step_name = local_path.split('/')[6]
        prod_file_name = 'Dropbox_%s' % step_name  # nazwa dla wystapienia w Prod Files
        task_name = 'Dropbox_%s_task' % step_name  # nazwa dla tasku
        sg_step = self.__get_step(step_name)
        creator = self.__get_creator(sg_script_name)    # pobiera zmienna ze zmiennych globalnych
        prod_file = self.__get_prod_file(project, prod_file_name, creator)
        version = self.__get_version(project, file_name, prod_file, local_path, creator)
        task = self.__get_task(project, task_name, prod_file, sg_step, creator)
        if version:
            del_version = sg.delete('Version', version['id'])
            deleted_entities['version'] = del_version
        if prod_file:
            del_prod_file = self.__delete_prod_file(prod_file)
            deleted_entities['prod_file'] = del_prod_file
        if task:
            del_task = self.__del_task(task)
            deleted_entities['task'] = del_task
        return deleted_entities

    def __create_version(self, project, prod_file, task, file_name, local_path):
        accepted_formats = ['jpg', 'png', 'gif', 'tif', 'tiff', 'bmp', 'exr', 'dpx', 'tga']
        file_format = file_name.split('.')[-1]
        sg_path_to_frame = self.__get_path_to_frame(local_path)
        data_version = {
            'project': {'type': 'Project', 'id': project['id']},
            'code': file_name,
            'sg_path_to_frames': sg_path_to_frame,
            'entity': {'type': 'CustomEntity04', 'id': prod_file['id']},
            'sg_task': {'type': 'Task', 'id': task['id']},
        }
        filters = [
            ['code', 'is', file_name],
            ['sg_path_to_frames', 'is', local_path],
            ['project', 'is', project],
        ]
        version = sg.find_one('Version', filters)
        if version is None:
            version = sg.create('Version', data_version)
            if file_format in accepted_formats:
                sg.upload_thumbnail('Version', version['id'], local_path)
            else:   # tutaj mozna wstawic dodawanie np obrazka z jakims logo dla innego formatu
                print('Cannot create version thumbnail for %s file format' % file_format)
        else:   # blok kodu wykonywany w wypadku istnienia wersji
            now = datetime.now()
            now = now.strftime('%d/%m/%Y %H:%M')
            description = 'Modified %s' % now
            data_version ={
                'description': description,
            }
            if file_format in accepted_formats:
                sg.upload_thumbnail('Version', version['id'], local_path)
            sg.update('Version', version['id'], data_version)
        return version

    @staticmethod
    def __get_path_to_frame(path):
        check_path = path.split('/')
        if check_path[1] == 'tmp':
            return None
        else:
            path = path.replace(sg_project_location, sg_project_mapped_loc)
            return path

    @staticmethod
    def __is_folder_to_skip(path):
        folder = path.split('/')[-2]
        folder = folder.lower()
        if folder in folders_to_skip:
            return True
        else:
            return False

    @staticmethod
    def __delete_prod_file(prod_file):
        prod_file_ver = sg.find('Version', [['entity', 'is', prod_file]])
        print('Prod files entity: %s' % prod_file_ver)
        if not prod_file_ver:
            prod_file_id = prod_file['id']
            sg.delete('CustomEntity04', prod_file_id)
            print('Deleted sg prod files: %s' % prod_file)
            return prod_file
        else:
            print('Prod file %s is not empty' % prod_file)

    @staticmethod
    def __del_task(task):
        task_versions = sg.find('Version', [['sg_task', 'is', task]])
        task_published_files = sg.find('PublishedFile', [['task', 'is', task]])
        if not task_versions and not task_published_files:
            task_id = task['id']
            sg.delete('Task', task_id)
            print('Deleted sg task: %s' % task)
            return task
        else:
            print('Task %s is not empty' % task)

    @staticmethod
    def __get_creator(script_name):
        creator_filter = [
            ['firstname', 'is', script_name],
        ]
        creator = sg.find_one('ApiUser', creator_filter)
        return creator

    @staticmethod
    def __get_prod_file(project, prod_file_name, creator):
        prod_file_filter = [
            ['project', 'is', project],
            ['code', 'is', prod_file_name],
            ['created_by', 'is', creator],
        ]
        prod_file = sg.find_one('CustomEntity04', prod_file_filter)
        return prod_file

    @staticmethod
    def __get_version(project, file_name, prod_file, local_path, creator):
        version_filter = [
            ['project', 'is', project],
            ['code', 'is', file_name],
            ['entity', 'is', prod_file],
            ['sg_path_to_frames', 'is', local_path],
            ['created_by', 'is', creator],
        ]
        version = sg.find_one('Version', version_filter)
        return version

    @staticmethod
    def __get_task(project, task_name, prod_file, sg_step, creator):
        task_filter = [
            ['project', 'is', project],
            ['content', 'is', task_name],
            ['entity', 'is', prod_file],
            ['step', 'is', sg_step],
            ['created_by', 'is', creator],
        ]
        task = sg.find_one('Task', task_filter)
        return task

    @staticmethod
    def __get_step(step):
        filters = [
            ['entity_type', 'is', 'CustomEntity04'],
            ['code', 'is', step],
        ]
        sg_step = sg.find_one('Step', filters)
        return sg_step

    @staticmethod
    def __convert_local_path(local_path):
        local_path = local_path.split('/')
        local_path = '/'.join(local_path[4:])
        local_path = 'X:/%s' % local_path
        return local_path

    @staticmethod
    def __create_prod_file(project, prod_file_name, step_name):
        data_prod_file = {
            'code': prod_file_name,
            'project': {'type': 'Project', 'id': project['id']},
            'description': 'Dropbox synced files for Prod Files step: %s' % step_name,
        }
        prod_file = sg.find_one('CustomEntity04', [['code', 'is', prod_file_name], ['project', 'is', project]])
        if prod_file is None:
            prod_file = sg.create('CustomEntity04', data_prod_file)
        else:
            print("Production files entity exist")
        return prod_file

    @staticmethod
    def __create_task(project, prod_file, task_name, sg_step):
        data_task = {
            'content': task_name,
            'project': {'type': 'Project', 'id': project['id']},
            'entity': {'type': 'CustomEntity04', 'id': prod_file['id']},
            'step': sg_step,
            'task_assignees': [{'type': 'Group', 'id': 4}],
            'sg_description': 'Task for Dropbox synced files',
        }
        task = sg.find_one('Task', [['content', 'is', task_name], ['project', 'is', project]])
        if task is None:
            task = sg.create('Task', data_task)
        else:
            print('Task exist')
        return task

    @staticmethod
    def __empty_tmp(folder):
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))


def main():
    active_projects = ProjectsStatus('Active')
    pitch_projects = ProjectsStatus('Pitch')
    while True:
        active_projects.update()
        active_projects_list = active_projects.projects
        pitch_projects.update()
        pitch_projects_list = pitch_projects.projects

        active_sync_dbx = SyncDropbox(active_projects_list)     # tworzy klase odpowiedzialna za zgranie plikow z dbx
        pitch_sync_dbx = SyncDropbox(pitch_projects_list)
        active_dbx_new_files = active_sync_dbx.check_dbx_files()  # szuka czy sa nowe pliki na dbx
        pitch_dbx_new_files = pitch_sync_dbx.check_dbx_files()
        active_sync_dbx.copy_files_from_dbx(active_dbx_new_files)  # zgrywa nowe pliki na dysk
        print('ACTIVE PROJECTS - DBX NEW FILES')
        print(active_dbx_new_files)
        print('PITCH PROJECTS - DBX NEW FILES')
        print(pitch_dbx_new_files)

        active_sync_sg = SyncSG(active_dbx_new_files)
        pitch_sync_sg = SyncSG(pitch_dbx_new_files)
        active_sg_new_entities = active_sync_sg.create_entities()  # towrzy wpisy na sg na pdostawie listy nowych plikow
        pitch_sg_new_entities = pitch_sync_sg.create_entities()
        print('ACTIVE PROJECTS - SG NEW ENTITIES')
        print(active_sg_new_entities)
        print('PITCH PROJECTS - SG NEW ENTITIES')
        print(pitch_sg_new_entities)


if __name__ == '__main__':
    main()
