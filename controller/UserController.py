"""
User Controller
Handles authentication and user management
FIXED: Now properly uses Service layer instead of direct repository access
"""

from services.UserService import UserService
from services.AuthService import AuthService
from core.DatabaseConnection import get_db_connection
from repositories.UserRepository import UserRepository, DepartmentRepository

class UserController:
    """
    Controller for user and authentication operations.
    Delegates all business logic to UserService and AuthService.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(UserController, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        # Initialize repositories
        db = get_db_connection()
        user_repo = UserRepository(db)
        dept_repo = DepartmentRepository(db)

        # Initialize services
        self._user_service = UserService(user_repo, dept_repo)
        self._auth_service = AuthService(user_repo)
        self._initialized = True

    # ========== AUTHENTICATION ==========

    def authenticate(self, username, password):
        """Authenticate user and return user data with permissions through service"""
        try:
            return self._auth_service.authenticate(username, password)
        except Exception as e:
            return None

    def check_permission(self, user_id, permission_type):
        """Check if user has specific permission through service"""
        try:
            return self._auth_service.check_permission(user_id, permission_type)
        except Exception as e:
            return False

    def get_user_permission_level(self, user_id):
        """Get user's permission level (1-10) through service"""
        try:
            return self._auth_service.get_user_permission_level(user_id)
        except Exception as e:
            return 0

    def get_user_permissions(self, user_id):
        """Get all permissions for a user through service"""
        try:
            return self._auth_service.get_user_permissions(user_id)
        except Exception as e:
            return {}

    # ========== USER MANAGEMENT ==========

    def get_all_users(self):
        """Get all users with role and department info through service"""
        try:
            return self._user_service.get_all_users()
        except Exception as e:
            return []

    def get_all_roles(self):
        """Get all users with role and department info through service"""
        try:
            return self._user_service.get_all_roles()
        except Exception as e:
            return []

    def get_user_by_id(self, user_id):
        """Get user by ID through service"""
        try:
            return self._user_service.get_user_by_id(user_id)
        except Exception as e:
            return None

    def get_user_by_username(self, username):
        """Get user by username through repository (read-only)"""
        try:
            return self._user_service.user_repo.find_by_username(username)
        except Exception as e:
            return None

    def create_user(self, username, email, password, full_name, role_id,
                    department_id=None, phone_number=None):
        """Create new user through service"""
        try:
            result = self._user_service.create_user(
                username=username,
                email=email,
                password=password,
                full_name=full_name,
                role_id=role_id,
                department_id=department_id,
                phone_number=phone_number
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def update_user(self, user_id, full_name=None, email=None,
                    phone_number=None, is_active=None):
        """Update user information through service"""
        try:
            result = self._user_service.update_user(
                user_id=user_id,
                full_name=full_name,
                email=email,
                phone_number=phone_number,
                is_active=is_active
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def activate_user(self, user_id):
        """Activate user account through service"""
        try:
            result = self._user_service.activate_user(user_id)
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def deactivate_user(self, user_id):
        """Deactivate user account through service"""
        try:
            result = self._user_service.deactivate_user(user_id)
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def delete_user(self, user_id, confirm=False):
        """Delete user through service"""
        try:
            result = self._user_service.delete_user(user_id, confirm)
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    # ========== DEPARTMENT OPERATIONS ==========

    def get_all_departments(self):
        """Get all departments with user counts through service"""
        try:
            return self._user_service.get_all_departments()
        except Exception as e:
            return []

    def get_department_by_id(self, department_id):
        """Get department by ID through service"""
        try:
            return self._user_service.get_department_by_id(department_id)
        except Exception as e:
            return None

    def create_department(self, department_name, description=None):
        """Create new department through service"""
        try:
            result = self._user_service.create_department(
                department_name=department_name,
                description=description
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def update_department(self, department_id, department_name=None, description=None):
        """Update department through service"""
        try:
            result = self._user_service.update_department(
                department_id=department_id,
                department_name=department_name,
                description=description
            )
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }

    def delete_department(self, department_id):
        """Delete department through service"""
        try:
            result = self._user_service.delete_department(department_id)
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }


def get_user_controller():
    """Get singleton instance"""
    return UserController()