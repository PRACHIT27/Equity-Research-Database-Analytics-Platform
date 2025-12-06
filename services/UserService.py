"""
User Service Layer
Business logic for user management and departments
NO SQL QUERIES - All database access through repositories
"""

from typing import Dict, Any, Optional
from repositories.UserRepository import UserRepository, DepartmentRepository
from utils.validators import UserValidator
from utils.exceptions import ValidationError, BusinessLogicError

class UserService:
    """Service for user management business logic"""

    def __init__(self, user_repo: UserRepository, dept_repo: DepartmentRepository):
        """
        Initialize user service.

        Args:
            user_repo: UserRepository instance
            dept_repo: DepartmentRepository instance
        """
        self.user_repo = user_repo
        self.dept_repo = dept_repo
        self.validator = UserValidator()


    def create_user(self, username: str, email: str, password: str,
                    full_name: str, role_id: int, department_id: Optional[int] = None,
                    phone_number: Optional[str] = None) -> Dict[str, Any]:
        """
        Create new user with validation.

        Args:
            username: Username
            email: Email address
            password: Plain text password (will be hashed in repo)
            full_name: Full name
            role_id: Role ID
            department_id: Department ID (optional)
            phone_number: Phone number (optional)

        Returns:
            dict: Success message and created user data

        Raises:
            ValidationError: If validation fails
            BusinessLogicError: If business rules violated
        """
        # Validate inputs using validators (business logic - OK here!)
        self.validator.validate_username(username)
        self.validator.validate_email(email)
        self.validator.validate_password(password)

        if not full_name or len(full_name) < 2:
            raise ValidationError("Full name must be at least 2 characters")

        # Check if username already exists (call repository - NO SQL!)
        existing_user = self.user_repo.find_by_username(username)
        if existing_user:
            raise BusinessLogicError(f"Username '{username}' is already taken")

        # Check if email already exists (call repository - NO SQL!)
        existing_email = self.user_repo.find_by_email(email)
        if existing_email:
            raise BusinessLogicError(f"Email '{email}' is already registered")

        # Validate role exists (call repository - NO SQL!)
        role = self.user_repo.find_role_by_id(role_id)
        if not role:
            raise BusinessLogicError(f"Role with ID {role_id} does not exist")

        # Validate department if provided (call repository - NO SQL!)
        if department_id:
            department = self.dept_repo.find_by_id(department_id)
            if not department:
                raise BusinessLogicError(f"Department with ID {department_id} does not exist")

        # Create user (call repository - NO SQL!)
        rows = self.user_repo.create(
            username=username,
            email=email,
            password=password,
            full_name=full_name,
            role_id=role_id,
            department_id=department_id,
            phone_number=phone_number
        )

        if rows == 0:
            raise BusinessLogicError("Failed to create user")

        # Retrieve created user (call repository - NO SQL!)
        created_user = self.user_repo.find_by_username(username)

        return {
            'success': True,
            'message': f"User '{username}' created successfully",
            'data': created_user
        }

    def get_all_users(self):
        """Get all users with details (call repository - NO SQL!)"""
        return self.user_repo.find_all_with_details()

    def get_all_roles(self):
        """Get all users with details (call repository - NO SQL!)"""
        return self.user_repo.get_all_roles()

    def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get user by ID (call repository - NO SQL!)"""
        user = self.user_repo.find_by_id_full(user_id)

        if not user:
            raise BusinessLogicError(f"User with ID {user_id} not found")

        return user

    def update_user(self, user_id: int, full_name: Optional[str] = None,
                    email: Optional[str] = None, phone_number: Optional[str] = None,
                    is_active: Optional[bool] = None) -> Dict[str, Any]:
        """
        Update user information.

        Args:
            user_id: User ID
            full_name: New full name
            email: New email
            phone_number: New phone
            is_active: Active status

        Returns:
            dict: Success message
        """
        # Check user exists (call repository - NO SQL!)
        existing = self.user_repo.find_by_id_full(user_id)

        if not existing:
            raise BusinessLogicError(f"User with ID {user_id} not found")

        # Validate new email if provided
        if email and email != existing['email']:
            self.validator.validate_email(email)

            # Check if new email already used (call repository - NO SQL!)
            email_check = self.user_repo.find_by_email(email)
            if email_check and email_check['user_id'] != user_id:
                raise BusinessLogicError(f"Email '{email}' is already in use")

        # Build update dict (business logic - OK here!)
        updates = {}
        if full_name is not None:
            updates['username'] = full_name
        if email is not None:
            updates['email'] = email
        if phone_number is not None:
            updates['phone_number'] = phone_number
        if is_active is not None:
            updates['is_active'] = is_active

        if not updates:
            raise ValidationError("No fields to update")

        # Update through repository (NO SQL!)
        rows = self.user_repo.update_by_id(user_id, **updates)

        if rows == 0:
            raise BusinessLogicError("No changes made")

        return {
            'success': True,
            'message': "User updated successfully"
        }

    def activate_user(self, user_id: int) -> Dict[str, Any]:
        """Activate user account (call repository - NO SQL!)"""
        rows = self.user_repo.activate_user(user_id)

        if rows == 0:
            raise BusinessLogicError("User not found")

        return {'success': True, 'message': "User activated"}

    def deactivate_user(self, user_id: int) -> Dict[str, Any]:
        """Deactivate user account (call repository - NO SQL!)"""
        rows = self.user_repo.deactivate_user(user_id)

        if rows == 0:
            raise BusinessLogicError("User not found")

        return {'success': True, 'message': "User deactivated"}

    def delete_user(self, user_id: int, confirm: bool = False) -> Dict[str, Any]:
        """
        Delete user with confirmation.

        Args:
            user_id: User ID to delete
            confirm: Confirmation flag

        Returns:
            dict: Success message
        """
        if not confirm:
            raise ValidationError("User deletion must be confirmed")

        print(user_id)
        # Check user exists (call repository - NO SQL!)
        user = self.user_repo.find_by_id_full(user_id)

        print(user)

        if not user:
            raise BusinessLogicError(f"User with ID {user_id} not found")

        # Delete user (call repository - NO SQL!)
        rows = self.user_repo.delete_by_id(user_id)

        print(rows)

        if rows == 0:
            raise BusinessLogicError("Failed to delete user")

        return {
            'success': True,
            'message': f"User '{user['username']}' deleted successfully"
        }

    # ========== DEPARTMENT OPERATIONS ==========

    def get_all_departments(self):
        """Get all departments with user counts (call repository - NO SQL!)"""
        return self.dept_repo.get_with_user_counts()

    def get_active_dept(self):
        """Get all departments with user counts (call repository - NO SQL!)"""
        return self.dept_repo.get_dept_count()

    def get_department_by_id(self, department_id: int) -> Dict[str, Any]:
        """Get department by ID (call repository - NO SQL!)"""
        dept = self.dept_repo.find_by_id(department_id)

        if not dept:
            raise BusinessLogicError(f"Department with ID {department_id} not found")

        return dept

    def create_department(self, department_name: str, description: Optional[str] = None) -> Dict[str, Any]:
        """
        Create new department.

        Args:
            department_name: Department name
            description: Department description

        Returns:
            dict: Success message
        """
        if not department_name or len(department_name) < 2:
            raise ValidationError("Department name must be at least 2 characters")

        # Check if exists (call repository - NO SQL!)
        existing = self.dept_repo.find_by_name(department_name)
        if existing:
            raise BusinessLogicError(f"Department '{department_name}' already exists")

        # Create (call repository - NO SQL!)
        rows = self.dept_repo.create(department_name, description)

        return {
            'success': True,
            'message': f"Department '{department_name}' created successfully"
        }

    def update_department(self, department_id: int, department_name: Optional[str] = None,
                          description: Optional[str] = None) -> Dict[str, Any]:
        """Update department (call repository - NO SQL!)"""

        # Check exists (call repository - NO SQL!)
        existing = self.dept_repo.find_by_id(department_id)
        if not existing:
            raise BusinessLogicError("Department not found")

        # Validate new name doesn't conflict
        if department_name and department_name != existing['department_name']:
            name_check = self.dept_repo.find_by_name(department_name)
            if name_check:
                raise BusinessLogicError(f"Department name '{department_name}' already exists")

        # Update (call repository - NO SQL!)
        rows = self.dept_repo.update_by_id(
            department_id,
            department_name=department_name,
            description=description
        )

        return {'success': True, 'message': "Department updated"}

    def delete_department(self, department_id: int) -> Dict[str, Any]:
        """Delete department (call repository - NO SQL!)"""

        # Check exists (call repository - NO SQL!)
        existing = self.dept_repo.find_by_id(department_id)
        if not existing:
            raise BusinessLogicError("Department not found")

        # Delete (call repository - NO SQL!)
        # Note: Users in this dept will have dept_id set to NULL (ON DELETE SET NULL)
        rows = self.dept_repo.delete_by_id(department_id)

        return {'success': True, 'message': "Department deleted"}